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

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.node.MLlibNodeSettings;
import com.knime.bigdata.spark.core.node.SparkModelLearnerNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec;
import com.knime.bigdata.spark.core.util.SparkIDs;
import com.knime.bigdata.spark.node.mllib.clustering.assigner.MLlibClusterAssignerNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibKMeansNodeModel extends SparkModelLearnerNodeModel<KMeansJobInput, MLlibNodeSettings> {

    /**
     * Name by which to refer to the KMeans models this node creates.
     */
    public static final String MODEL_NAME = "KMeans";

    private final SettingsModelIntegerBounded m_noOfCluster = createNoOfClusterModel();

    private final SettingsModelIntegerBounded m_noOfIteration = createNoOfIterationModel();

    /** The unique Spark job id. */
    public static final String JOB_ID = MLlibKMeansNodeModel.class.getCanonicalName();

    /**
     *
     */
    public MLlibKMeansNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkModelPortObject.TYPE}, MODEL_NAME, JOB_ID, false);
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
        //call super to check the column names
        super.configureInternal(inSpecs);
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec tableSpec = spec.getTableSpec();
        final SparkDataPortObjectSpec asignedSpec =
            new SparkDataPortObjectSpec(spec.getContextID(), createResultTableSpec(tableSpec));
        final SparkModelPortObjectSpec modelSpec = createMLSpec(spec);
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
        final JobRunFactory<KMeansJobInput, ModelJobOutput> runFactory = getJobRunFactory(data);
        exec.setMessage("Starting KMeans (SPARK) Learner");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final MLlibNodeSettings settings = getSettings();
        final DataTableSpec resultSpec = createResultTableSpec(tableSpec);
        final KMeansJobInput jobInput = createJobInput(inObjects, settings);
        final ModelJobOutput result = runFactory.createRun(jobInput).run(data.getContextID(), exec);
        exec.setMessage("KMeans (SPARK) Learner done.");
        return new PortObject[]{createSparkPortObject(data, resultSpec, jobInput.getFirstNamedOutputObject()),
            createSparkModelPortObject(inObjects, settings, result)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected KMeansJobInput createJobInput(final PortObject[] inData, final MLlibNodeSettings settings)
        throws InvalidSettingsException {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final String aOutputTableName = SparkIDs.createRDDID();
        final KMeansJobInput jobInput = new KMeansJobInput(data.getTableName(), aOutputTableName,
            Arrays.asList(settings.getSettings(data).getFeatueColIdxs()), m_noOfCluster.getIntValue(), m_noOfIteration.getIntValue());
        return jobInput;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_noOfCluster.saveSettingsTo(settings);
        m_noOfIteration.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfCluster.validateSettings(settings);
        m_noOfIteration.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfCluster.loadSettingsFrom(settings);
        m_noOfIteration.loadSettingsFrom(settings);
    }
}
