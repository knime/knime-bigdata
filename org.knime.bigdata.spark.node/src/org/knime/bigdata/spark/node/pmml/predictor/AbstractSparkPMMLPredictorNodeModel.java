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
 *   Created on 16.05.2016 by koetter
 */
package org.knime.bigdata.spark.node.pmml.predictor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Map;

import org.knime.base.node.mine.util.PredictorHelper;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobWithFilesRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.util.SparkPMMLUtil;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkPMMLPredictorNodeModel extends SparkNodeModel {

    /**The unique Spark job id.*/
    public static final String JOB_ID = AbstractSparkPMMLPredictorNodeModel.class.getCanonicalName();

    private static final String CFG_KEY_OUTPROP = "outProp";
    private SettingsModelBoolean m_outputProbabilities = createOutputProbabilitiesSettingsModel();
    private SettingsModelBoolean m_changePredColName = PredictorHelper.getInstance().createChangePrediction();
    private SettingsModelString m_predColName = PredictorHelper.getInstance().createPredictionColumn();
    private SettingsModelString m_suffix = PredictorHelper.getInstance().createSuffix();

    /**
     * Creates the settings model that determines whether the node should output the probabilities for each class.
     * @return the settings model
     */
    public static SettingsModelBoolean createOutputProbabilitiesSettingsModel() {
        return new SettingsModelBoolean(CFG_KEY_OUTPROP, false);
    }

    /**
     * @param inPortTypes
     * @param outPortTypes
     */
    protected AbstractSparkPMMLPredictorNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * @param inPortTypes
     * @param outPortTypes
     * @param deleteOnReset
     */
    public AbstractSparkPMMLPredictorNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes, final boolean deleteOnReset) {
        super(inPortTypes, outPortTypes, deleteOnReset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[1];
        final String predColName = m_changePredColName.getBooleanValue() ? m_predColName.getStringValue() : null;
        final DataTableSpec resultSpec = getResultSpec(inSpecs[0], sparkSpec, predColName,
            m_outputProbabilities.getBooleanValue(), m_suffix.getStringValue());
        if (resultSpec == null) {
            return new PortObjectSpec[] {null};
        }
        return new PortObjectSpec[] {new SparkDataPortObjectSpec(sparkSpec.getContextID(), resultSpec)};
    }
    /**
     * @param pmmlSpec
     * @param sparkSpec
     * @param predColname
     * @param outputProbabilities
     * @param suffix
     * @return the {@link DataTableSpec} or <code>null</code>
     */
    protected abstract DataTableSpec getResultSpec(final PortObjectSpec pmmlSpec,
        final SparkDataPortObjectSpec sparkSpec, String predColname, boolean outputProbabilities, String suffix);

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final CompiledModelPortObject pmml = getCompiledModel(inObjects[0]);
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[1];
        final IntermediateSpec inputSchema = SparkDataTableUtil.toIntermediateSpec(data.getTableSpec());
        final CompiledModelPortObjectSpec cms = (CompiledModelPortObjectSpec)pmml.getSpec();
        final String predColName = m_changePredColName.getBooleanValue() ? m_predColName.getStringValue() : null;
        final DataTableSpec resultSpec = SparkPMMLUtil.createPredictionResultSpec(data.getTableSpec(), cms,
            predColName, m_outputProbabilities.getBooleanValue(), m_suffix.getStringValue());
        final String aOutputTableName = SparkIDs.createSparkDataObjectID();
        final IntermediateSpec outputSchema = SparkDataTableUtil.toIntermediateSpec(resultSpec);
        final Integer[] colIdxs = SparkPMMLUtil.getColumnIndices(data.getTableSpec(), cms);
        final File jobFile = createJobFile(pmml);
        addFileToDeleteAfterExecute(jobFile);
        final PMMLPredictionJobInput input = new PMMLPredictionJobInput(data.getTableName(), inputSchema, colIdxs,
            pmml.getModelClassName(), aOutputTableName, outputSchema, m_outputProbabilities.getBooleanValue());
        final JobWithFilesRunFactory<PMMLPredictionJobInput, EmptyJobOutput> execProvider =
                SparkContextUtil.getJobWithFilesRunFactory(data.getContextID(), JOB_ID);
        execProvider.createRun(input, Collections.singletonList(jobFile)).run(data.getContextID(), exec);
        return new PortObject[] {createSparkPortObject(data, resultSpec, aOutputTableName)};
    }

    /**
     * @param pmml
     * @return the serialized pmml class
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static File createJobFile(final CompiledModelPortObject pmml) throws IOException, FileNotFoundException {
        final Map<String, byte[]> bytecode = pmml.getBytecode();
        File outFile = File.createTempFile("knime-pmml", ".tmp");
        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)))) {
            out.writeObject(bytecode);
        }
        return outFile;
    }

    /**
     * @param inObject the pmml input object
     * @return the {@link CompiledModelPortObject}
     * @throws Exception if the input object can not be converted to a {@link CompiledModelPortObject}
     */
    protected abstract CompiledModelPortObject getCompiledModel(final PortObject inObject) throws Exception;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_changePredColName.saveSettingsTo(settings);
        m_outputProbabilities.saveSettingsTo(settings);
        m_predColName.saveSettingsTo(settings);
        m_suffix.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_changePredColName.loadSettingsFrom(settings);
        m_outputProbabilities.loadSettingsFrom(settings);
        m_predColName.loadSettingsFrom(settings);
        m_suffix.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_changePredColName.validateSettings(settings);
        m_outputProbabilities.validateSettings(settings);
        m_predColName.validateSettings(settings);
        m_suffix.validateSettings(settings);
    }

}