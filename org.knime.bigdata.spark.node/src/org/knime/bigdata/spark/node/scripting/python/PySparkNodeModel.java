/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Mar 7, 2017 by sascha
 */
package org.knime.bigdata.spark.node.scripting.python;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.scripting.python.util.FlowVariableCleaner;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkDocument;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkHelper;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkHelperRegistry;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkJobInput;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkJobOutput;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;

/**
 * DataFrame-based PySpark node model.
 *
 * @author Mareike Hoeger, KNIME GmbH
 */
public class PySparkNodeModel extends SparkNodeModel {

    /** Unique job id */
    public static final String JOB_ID = "PySparkJob";

    private final PySparkNodeConfig m_config;

    private int m_inputCount;

    private final int m_outputCount;

    /**
     * Default constructor.
     *
     * @param inPortTypes the list of input PortTypes
     * @param outPortTypes the list of output PortTypes
     */
    public PySparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
        if ((inPortTypes.length == 1) && inPortTypes[0].equals(SparkContextPortObject.TYPE)) {
            //This is a source node
            m_inputCount = 0;

        } else {
            m_inputCount = inPortTypes.length;
        }
        m_outputCount = outPortTypes.length;
        m_config = new PySparkNodeConfig(m_inputCount, m_outputCount);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final SparkVersion sparkVersion = getSparkVersion(inSpecs);
        final PySparkHelper helper = getPySparkHelper(sparkVersion);

        final PySparkDocument doc = (PySparkDocument)m_config.getDoc();
        updateFlowVariables(doc);
        helper.updateGuardedSection(doc, m_inputCount, m_outputCount);
        helper.checkUDF(doc, m_outputCount);

        checkSparkContextIDs(inSpecs);

        return null;
    }

    private static PySparkHelper getPySparkHelper(final SparkVersion sparkVersion) throws InvalidSettingsException {
        final PySparkHelper helper = getHelperRegistry().getHelper(sparkVersion);
        if ((helper == null) || !helper.supportSpark(sparkVersion)) {
            throw new InvalidSettingsException(String.format("Spark Version %s not supported.", sparkVersion));
        }
        return helper;
    }

    private static void checkSparkContextIDs(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        SparkContextID firstContextID = null;
        for (final PortObjectSpec inSpec : inSpecs) {
            if ((inSpec != null) && (inSpec instanceof SparkContextProvider)) {
                final SparkContextID currContextID = ((SparkContextProvider)inSpec).getContextID();
                if (firstContextID == null) {
                    firstContextID = currContextID;
                } else if (!firstContextID.equals(currContextID)) {
                    throw new InvalidSettingsException("Input objects belong to two different Spark contexts");
                }
            }
        }
    }

    private void updateFlowVariables(final PySparkDocument doc) {
        final Map<String, FlowVariable> flowVariables = getAvailableFlowVariables();
        final List<FlowVariable> usedFlowVariables = new ArrayList<>();
        for (final String name : m_config.getUsedFlowVariablesNames()) {
            if (flowVariables.containsKey(name)) {
                usedFlowVariables.add(flowVariables.get(name));
            }
        }
        doc.writeFlowVariables(usedFlowVariables.toArray(new FlowVariable[usedFlowVariables.size()]));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {

        final String warningMessage =
            FlowVariableCleaner.cleanFlowVariables(m_config, new ArrayList<>(getAvailableFlowVariables().values()));
        if (!warningMessage.isEmpty()) {
            setWarningMessage(warningMessage);
        }
        final PySparkDocument doc = (PySparkDocument)m_config.getDoc();
        updateFlowVariables(doc);

        return executeScript(inData, m_inputCount, m_outputCount, (PySparkDocument)m_config.getDoc(), exec, -1);
    }

    /**
     * Executes the PySpark script with the given input objects and creates the given number of output objects
     *
     * @param inData the input data
     * @param inputCount the number of InputObjects
     * @param doc the Document that contains the user code
     * @param outputCount the number of OutputObjects to create
     * @param exec the execution context
     * @param numRows the number of rows to execute on -1 for all rows
     * @return an array of output port objects depending on the <code>numOutputObjects</code>
     * @throws Exception if an error occurs in the Spark job execution
     */
    public static PortObject[] executeScript(final PortObject[] inData, final int inputCount, final int outputCount,
        final PySparkDocument doc, final ExecutionMonitor exec, final int numRows) throws Exception {

        final SparkContextID contextID = getContextID(inData);
        final String dataFrame1 = getDataFrameIDFromPortObjects(inData, 0);
        final String dataFrame2 = getDataFrameIDFromPortObjects(inData, 1);

        final String[] outputObjects = createOutputObjects(outputCount);

        final String dataFramePrefix = UUID.randomUUID().toString();
        final PySparkHelper helper = getHelperRegistry().getHelper(getSparkVersion(inData));
        helper.updateGuardedSectionsUIDs(doc, inputCount, outputCount, dataFramePrefix);
        final String code = doc.getText(0, doc.getLength());

        final PySparkJobInput input = new PySparkJobInput(dataFrame1, dataFrame2, outputObjects[0], outputObjects[1],
            code, dataFramePrefix, numRows);
        final PySparkJobOutput output =
            SparkContextUtil.<PySparkJobInput, PySparkJobOutput> getJobRunFactory(contextID, JOB_ID).createRun(input)
                .run(contextID, exec);

        return createOutputPortObjects(outputCount, contextID, outputObjects, output);
    }

    private static String[] createOutputObjects(final int outputCount) {
        final String[] outputObjects = new String[]{null, null};
        for (int i = 0; i < outputCount; i++) {
            outputObjects[i] = SparkIDs.createSparkDataObjectID();
        }
        return outputObjects;
    }

    private static PortObject[] createOutputPortObjects(final int outputCount, final SparkContextID contextID,
        final String[] outputObjects, final PySparkJobOutput output) {
        final PortObject[] outputs = new PortObject[outputCount];
        for (int i = 0; i < outputCount; i++) {
            @SuppressWarnings("deprecation")
            final DataTableSpec knimeOutputSpec =
                KNIMEToIntermediateConverterRegistry.convertSpec(output.getSpec(outputObjects[i]));
            final SparkDataTable resultTable = new SparkDataTable(contextID, outputObjects[i], knimeOutputSpec);
            outputs[i] = new SparkDataPortObject(resultTable);
        }
        return outputs;
    }

    private static String getDataFrameIDFromPortObjects(final PortObject[] inData, final int index) {
        String dataFrameIDToReturn = null;

        if ((inData != null) && (index < inData.length) && (inData[index] instanceof SparkDataPortObject)) {
            dataFrameIDToReturn = ((SparkDataPortObject)inData[index]).getTableName();
        }

        return dataFrameIDToReturn;
    }

    /**
     * @param inData either the {@link PortObject} array in execute or the {@link PortObjectSpec} array in configure
     * @return the {@link SparkVersion} of the first Spark Context found in inData
     * @throws InvalidSettingsException
     */
    protected static SparkVersion getSparkVersion(final Object[] inData) throws InvalidSettingsException {
        return SparkContextUtil.getSparkVersion(getContextID(inData));
    }

    /**
     * @param inData either the {@link PortObject} array in execute or the {@link PortObjectSpec} array in configure
     * @return the first {@link SparkContextID} found in inData
     * @throws InvalidSettingsException
     */
    protected static SparkContextID getContextID(final Object[] inData) throws InvalidSettingsException {
        if (inData != null) {
            for (final Object in : inData) {
                if ((in != null) && (in instanceof SparkContextProvider)) {
                    return ((SparkContextProvider)in).getContextID();
                }
            }
        }

        throw new InvalidSettingsException("Spark input connection required");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_config.saveTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final PySparkNodeConfig config = new PySparkNodeConfig(m_inputCount, m_outputCount);
        config.loadFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config.loadFrom(settings);
    }

    /** @return snippet helper registry */
    protected static PySparkHelperRegistry getHelperRegistry() {
        return PySparkHelperRegistry.getInstance();
    }
}
