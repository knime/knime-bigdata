/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.node.mllib.clustering.kmeans;

import java.util.Arrays;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.node.MLlibNodeSettings;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import com.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.core.util.SparkIDs;
import com.knime.bigdata.spark.node.mllib.clustering.assigner.MLlibClusterAssignerNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibKMeansNodeModel extends SparkNodeModel {

    /**
     * Name by which to refer to the KMeans models this node creates.
     */
    public static final String MODEL_NAME = "KMeans";

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(false);

    private final SettingsModelIntegerBounded m_noOfCluster = createNoOfClusterModel();

    private final SettingsModelIntegerBounded m_noOfIteration = createNoOfIterationModel();

    /** The unique Spark job id. */
    public static final String JOB_ID = MLlibKMeansNodeModel.class.getCanonicalName();

    /**
     *
     */
    public MLlibKMeansNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkModelPortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelIntegerBounded createNoOfClusterModel() {
        return new SettingsModelIntegerBounded("noOfCluster", 3, 1, Integer.MAX_VALUE);
    }

    /**
     * @return
     */
    static SettingsModelIntegerBounded createNoOfIterationModel() {
        return new SettingsModelIntegerBounded("noOfIteration", 30, 1, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input found");
        }

        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec tableSpec = spec.getTableSpec();
        m_settings.check(tableSpec);
        final SparkDataPortObjectSpec asignedSpec =
            new SparkDataPortObjectSpec(spec.getContextID(), createResultTableSpec(tableSpec), getKNIMESparkExecutorVersion());
        final SparkModelPortObjectSpec modelSpec = new SparkModelPortObjectSpec(getSparkVersion(spec), MODEL_NAME);
        return new PortObjectSpec[]{asignedSpec, modelSpec};
    }

    /**
     * @param tableSpec
     * @return
     */
    private DataTableSpec createResultTableSpec(final DataTableSpec tableSpec) {
        return MLlibClusterAssignerNodeModel.createSpec(tableSpec, "Cluster");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        final SparkContextID contextId = data.getContextID();
        final JobRunFactory<KMeansJobInput, ModelJobOutput> runFactory = SparkContextUtil.getJobRunFactory(contextId, JOB_ID);
        exec.setMessage("Starting KMeans (SPARK) Learner");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final DataTableSpec resultSpec = createResultTableSpec(tableSpec);
        final IntermediateSpec resultInterSpec = SparkDataTableUtil.toIntermediateSpec(resultSpec, getKNIMESparkExecutorVersion());
        final KMeansJobInput jobInput = createJobInput(inObjects, resultInterSpec);
        final ModelJobOutput result = runFactory.createRun(jobInput).run(data.getContextID(), exec);
        exec.setMessage("KMeans (SPARK) Learner done.");
        return new PortObject[]{
            createSparkPortObject(data, resultSpec, jobInput.getFirstNamedOutputObject(),
                getKNIMESparkExecutorVersion()),
            createSparkModelPortObject(data, MODEL_NAME, m_settings.getSettings(data), result)};
    }

    private KMeansJobInput createJobInput(final PortObject[] inData, final IntermediateSpec outputSpec)
            throws InvalidSettingsException {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final String inputKey = data.getTableName();
        final String outputKey = SparkIDs.createSparkDataObjectID();
        final List<Integer> featureColumnIdxs = Arrays.asList(m_settings.getSettings(data).getFeatueColIdxs());
        final KMeansJobInput jobInput = new KMeansJobInput(inputKey, outputKey, outputSpec, featureColumnIdxs,
            m_noOfCluster.getIntValue(), m_noOfIteration.getIntValue());
        return jobInput;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
        m_noOfCluster.saveSettingsTo(settings);
        m_noOfIteration.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
        m_noOfCluster.validateSettings(settings);
        m_noOfIteration.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
        m_noOfCluster.loadSettingsFrom(settings);
        m_noOfIteration.loadSettingsFrom(settings);
    }
}
