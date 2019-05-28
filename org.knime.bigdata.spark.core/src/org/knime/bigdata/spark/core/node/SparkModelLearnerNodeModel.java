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
 *   Created on 07.05.2016 by koetter
 */
package org.knime.bigdata.spark.core.node;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <I> {@link JobInput} class
 * @param <S> the {@link MLlibNodeSettings}
 */
public abstract class SparkModelLearnerNodeModel<I extends JobInput, S extends MLlibNodeSettings> extends SparkNodeModel {

    private final String m_modelName;
    private final String m_jobId;
    private final S m_settings;

    /**
     * Default constructor with a single {@link SparkDataPortObject} as input and a single {@link SparkModelPortObject}a
     * as output port.
     * @param modelName the unique model name
     * @param jobId the unique job id
     * @param requireClassCol <code>true</code> if this model learner requires a class column
     */
    protected SparkModelLearnerNodeModel(final String modelName, final String jobId, final boolean requireClassCol) {
        this(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkModelPortObject.TYPE}, modelName, jobId, requireClassCol);
    }

    /**
     * Constructor for general input/output {@link PortType}s. if you use this constructor you might need to overwrite
     * the {@link #configureInternal(PortObjectSpec[])} and {@link #executeInternal(PortObject[], ExecutionContext)}
     * method which expects a {@link SparkDataPortObject} as first input and returns a {@link SparkModelPortObject} as
     * output.
     * @param inPortTypes the input {@link PortType}s
     * @param outPortTypes the output {@link PortType}s
     * @param modelName the unique model name
     * @param jobId the unique job id
     * @param requireClassCol <code>true</code> if this model learner requires a class column
     */
    @SuppressWarnings("unchecked")
    protected SparkModelLearnerNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final String modelName, final String jobId, final boolean requireClassCol) {
        this(inPortTypes, outPortTypes, modelName, jobId, (S)new MLlibNodeSettings(requireClassCol));
    }

    /**
     * Constructor for general input/output {@link PortType}s. if you use this constructor you might need to overwrite
     * the {@link #configureInternal(PortObjectSpec[])} and {@link #executeInternal(PortObject[], ExecutionContext)}
     * method which expects a {@link SparkDataPortObject} as first input and returns a {@link SparkModelPortObject} as
     * output.
     * @param inPortTypes the input {@link PortType}s
     * @param outPortTypes the output {@link PortType}s
     * @param modelName the unique model name
     * @param jobId the unique job id
     * @param settings
     */
    public SparkModelLearnerNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final String modelName, final String jobId, final S settings) {
        super(inPortTypes, outPortTypes);
        m_modelName = modelName;
        m_jobId = jobId;
        m_settings = settings;
    }

    /**
     * @return the unique job ID
     */
    protected String getJobId() {
        return m_jobId;
    }

    /**
     * @return the unique model name
     */
    protected String getModelName() {
        return m_modelName;
    }

    /**
     * {@inheritDoc}
     * Expects a {@link SparkDataPortObjectSpec} as first input
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input found");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec tableSpec = spec.getTableSpec();
        m_settings.check(tableSpec);
        return new PortObjectSpec[]{createMLSpec(spec)};
    }


    /**
     * @param spec
     * @return the {@link SparkModelPortObjectSpec}
     */
    protected SparkModelPortObjectSpec createMLSpec(final SparkContextProvider spec) {
        return new SparkModelPortObjectSpec(getSparkVersion(spec), getModelName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting " + getModelName() +" model learner");
        exec.checkCanceled();
        final S settings = getSettings();
        final ModelJobOutput result = executeSparkjob(exec, inData, settings);
        exec.setMessage(getModelName()+ " model learner done.");
        return new PortObject[]{createSparkModelPortObject(inData, settings, result)};
    }

    /**
     * @param inData the input {@link SparkDataPortObject}
     * @param settings the {@link MLlibSettings}
     * @param result the {@link ModelJobOutput}
     * @return the {@link SparkModelPortObject}
     * @throws MissingSparkModelHelperException if no model helper is registered for the given model name
     * @throws InvalidSettingsException
     */
    protected SparkModelPortObject createSparkModelPortObject(final PortObject[] inData,
        final S settings, final ModelJobOutput result) throws MissingSparkModelHelperException, InvalidSettingsException {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        return createSparkModelPortObject(data, getModelName(), settings.getSettings(data), result);
    }

    /**
     * @return the {@link MLlibNodeSettings}
     */
    protected S getSettings() {
        return m_settings;
    }

    /**
     * @param data the input {@link SparkDataPortObject}
     * @return the {@link MLlibSettings}
     * @throws InvalidSettingsException if the settings are invalid
     */
    protected MLlibSettings getMLlibSettings(final SparkDataPortObject data) throws InvalidSettingsException {
        final DataTableSpec tableSpec = data.getTableSpec();
        final MLlibSettings settings = m_settings.getSettings(tableSpec);
        return settings;
    }

    /**
     * @param exec {@link ExecutionMonitor}
     * @param inData the input {@link SparkDataPortObject}
     * @param settings the {@link MLlibSettings}
     * @return the {@link ModelJobOutput}
     * @throws KNIMESparkException indicates an exception in Spark
     * @throws CanceledExecutionException the user has canceled the execution
     * @throws InvalidSettingsException
     */
    protected ModelJobOutput executeSparkjob(final ExecutionMonitor exec, final PortObject[] inData,
        final S settings) throws KNIMESparkException, CanceledExecutionException, InvalidSettingsException {
        final I jobInput = createJobInput(inData, settings);
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final JobRunFactory<I, ModelJobOutput> jobProvider = SparkContextUtil.getJobRunFactory(data.getContextID(), getJobId());
        final ModelJobOutput result = jobProvider.createRun(jobInput).run(data.getContextID(), exec);
        return result;
    }

    /**
     * @param inData the input {@link SparkDataPortObject}
     * @param settings {@link MLlibSettings}
     * @return the {@link JobInput}
     * @throws InvalidSettingsException if the settings are invalid
     */
    protected abstract I createJobInput(final PortObject[] inData, final S settings)
            throws InvalidSettingsException;

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
        saveModelLearnerSettingsTo(settings);
    }

    /**
     * Overwrite this method to load additional settings.
     * @param settings
     */
    protected void saveModelLearnerSettingsTo(final NodeSettingsWO settings) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
        validateModelLearnerSettings(settings);
    }

    /**
     * @param settings
     * @throws InvalidSettingsException
     */
    protected void validateModelLearnerSettings(final NodeSettingsRO settings) throws InvalidSettingsException {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
        loadValidatedModelLearnerSettingsFrom(settings);
    }

    /**
     * @param settings
     * @throws InvalidSettingsException
     */
    protected void loadValidatedModelLearnerSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {}

}
