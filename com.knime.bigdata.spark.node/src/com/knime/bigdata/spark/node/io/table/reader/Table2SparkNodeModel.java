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
 *   Created on 26.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.io.table.reader;

import java.io.File;
import java.io.IOException;

import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.StreamableOperator;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import com.knime.bigdata.spark.node.SparkNodePlugin;

/**
 * Node model that uploads a a KNIME {@link DataTable} into a Spark cluster and provides a {@link SparkDataTable} that
 * references the (now remote) data.
 * <p>/
 * The input port of this node is streamable. Also, this node behaves differently when executed in KNIME-on-Spark mode.
 *
 * @author Tobias Koetter, KNIME.com
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class Table2SparkNodeModel extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Table2SparkNodeModel.class);

    /**The unique Spark job id.*/
    public static final String JOB_ID = "Table2SparkJob";

    private final SettingsModelString m_knospOutputID = new SettingsModelString("knospOutputID", null);

    /**
     * Default constructor.
     * @param optionalSparkPort true if input spark context port is optional
     */
    Table2SparkNodeModel(final boolean optionalSparkPort) {
        super(new PortType[]{BufferedDataTable.TYPE}, optionalSparkPort, new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }

        // convert KNIME spec into spark spec and back into KNIME spec
        final DataTableSpec inputSpec = (DataTableSpec)inSpecs[0];
        @SuppressWarnings("deprecation")
        // if you change this, you also need to change the behavior in the streamable operator implementation and in executeInternal()
        final DataTableSpec outputSpec = SparkDataTableUtil.toSparkOutputSpec(inputSpec, getKNIMESparkExecutorVersion());
        final SparkDataPortObjectSpec resultSpec =
            new SparkDataPortObjectSpec(getContextID(inSpecs), outputSpec, getKNIMESparkExecutorVersion());
        setConverterWarningMessage(inputSpec, outputSpec);

        return new PortObjectSpec[]{resultSpec};
    }

	private boolean isKNOSPMode() {
        return m_knospOutputID.getStringValue() != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length < 1 || inData[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        //Check that the context is available before doing all the work
        final SparkContextID contextID = getContextID(inData);
        final BufferedDataTable inputTable = (BufferedDataTable)inData[0];

        // if you change this, you also need to change the behavior in the streamable operator implementation and in configureInternal()
        @SuppressWarnings("deprecation")
        final DataTableSpec outputSpec = SparkDataTableUtil.toSparkOutputSpec(inputTable.getDataTableSpec(), getKNIMESparkExecutorVersion());
        setConverterWarningMessage(inputTable.getDataTableSpec(), outputSpec);

        final AbstractTable2SparkStreamableOperator streamableOp = createStreamableOperatorInternal(contextID);
        streamableOp.runWithRowInput(new DataTableRowInput(inputTable), exec);

        return new PortObject[]{new SparkDataPortObject(new SparkDataTable(contextID,
            streamableOp.getNamedOutputObjectId(), outputSpec, getKNIMESparkExecutorVersion()))};
    }

    /**
     * Clears or sets the warning node messages if type converters are missing.
     * @param inSpec current KNIME spec
     * @param outSpec KNIME spec after converting to Spark and back to KNIME
     */
    private void setConverterWarningMessage(final DataTableSpec inSpec, final DataTableSpec outSpec) {
        StringBuilder sb = new StringBuilder();
        int nodeWarnings = 0;

        for (int i = 0; i < inSpec.getNumColumns(); i++) {
            final DataType inType = inSpec.getColumnSpec(i).getType();
            final DataType outType = outSpec.getColumnSpec(i).getType();
            if (!inType.equals(outType)) {
                final String warning = String.format(
                    "Data type of column '%s' changes between Spark and KNIME (type before: %s, after: %s).\n",
                    inSpec.getColumnNames()[i], inType, outType);

                if (nodeWarnings < 5) { // limit warning messages
                    sb.append(warning);
                    nodeWarnings++;
                } else {
                    LOGGER.warn(warning);
                }
            }
        }

        if (sb.length() > 0) {
            setWarningMessage(sb.toString());
        } else {
            setWarningMessage(null);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
    	m_knospOutputID.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		if (settings.containsKey(m_knospOutputID.getKey())) {
            m_knospOutputID.validateSettings(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		if (settings.containsKey(m_knospOutputID.getKey())) {
            m_knospOutputID.loadSettingsFrom(settings);
        }
    }

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        return createStreamableOperatorInternal(getContextID(inSpecs));
    }

    private AbstractTable2SparkStreamableOperator createStreamableOperatorInternal(final SparkContextID contextID) {
        if (isKNOSPMode()) {
            // KNIME-on-Spark mode (running inside a JVM of a Spark executor in a cluster)
            return SparkNodePlugin.getKNOSPHelper().createTable2SparkStreamableOperator(m_knospOutputID.getStringValue(), getKNIMESparkExecutorVersion());
        } else {
            final File tmpFile = createTempFile();
            return new Table2SparkStreamableOperator(contextID, tmpFile, getKNIMESparkExecutorVersion());
        }
    }

    private File createTempFile() {
        // first we need to create a temp file that is deleted after executing this node
        final File tmpFile;
        try {
            tmpFile = File.createTempFile("knime-table2spark", ".tmp");
            LOGGER.debugWithFormat("Serializing data table to file %s", tmpFile.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp file: " + e.getMessage(), e);
        }
        addFileToDeleteAfterExecute(tmpFile);
        return tmpFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.NONDISTRIBUTED_STREAMABLE, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE};
    }

    /**
     * Puts this node model into KNIME-on-Spark (KNOSP) mode. In KNOSP mode, the ingoing KNIME data table will
     * not be uploaded to Spark, but written out to an in-memory data structure obtained using the provided key.
     *
     * @param knospOutputID a unique key under which an output queue will be looked up
     */
    public void activateKNOSPMode(final String knospOutputID) {
        m_knospOutputID.setStringValue(knospOutputID);
    }
}
