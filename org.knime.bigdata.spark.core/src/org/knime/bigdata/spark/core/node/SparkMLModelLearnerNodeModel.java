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
 *   Created on May 21, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.node;

import java.nio.file.Files;
import java.util.UUID;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.MLModelLearnerJobOutput;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.NamedModelLearnerJobInput;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObject;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObjectSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.filestore.FileStore;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Abstract superclass for all Spark model learners that learn "named models", i.e. models which are stored under an ID
 * on the Spark side and which can later be used for prediction without reuploading them.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @param <I> {@link NamedModelLearnerJobInput} class
 * @param <S> the {@link MLlibNodeSettings}
 */
public abstract class SparkMLModelLearnerNodeModel<I extends NamedModelLearnerJobInput, S extends MLlibNodeSettings>
    extends SparkModelLearnerNodeModel<I, S> {

    /**
     * Default constructor with a single {@link SparkDataPortObject} as input and a single {@link SparkModelPortObject}a
     * as output port.
     *
     * @param modelName the unique model name
     * @param jobId the unique job id
     * @param requireTargetCol <code>true</code> if this model learner requires a class column
     */
    protected SparkMLModelLearnerNodeModel(final String modelName, final String jobId,
        final boolean requireTargetCol) {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkMLModelPortObject.PORT_TYPE}, modelName,
            jobId, requireTargetCol);
    }

    /**
     * Constructor for general input/output {@link PortType}s. if you use this constructor you might need to overwrite
     * the {@link #configureInternal(PortObjectSpec[])} and {@link #executeInternal(PortObject[], ExecutionContext)}
     * method which expects a {@link SparkDataPortObject} as first input and returns a {@link SparkModelPortObject} as
     * output.
     *
     * @param inPortTypes the input {@link PortType}s
     * @param outPortTypes the output {@link PortType}s
     * @param modelName the unique model name
     * @param jobId the unique job id
     * @param requireTargetCol <code>true</code> if this model learner requires a class column
     */
    protected SparkMLModelLearnerNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final String modelName, final String jobId, final boolean requireTargetCol) {

        super(inPortTypes, outPortTypes, modelName, jobId, requireTargetCol);
    }

    /**
     * Constructor for general input/output {@link PortType}s. if you use this constructor you might need to overwrite
     * the {@link #configureInternal(PortObjectSpec[])} and {@link #executeInternal(PortObject[], ExecutionContext)}
     * method which expects a {@link SparkDataPortObject} as first input and returns a {@link SparkModelPortObject} as
     * output.
     *
     * @param inPortTypes the input {@link PortType}s
     * @param outPortTypes the output {@link PortType}s
     * @param modelName the unique model name
     * @param jobId the unique job id
     * @param settings
     */
    public SparkMLModelLearnerNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final String modelName, final String jobId, final S settings) {
        super(inPortTypes, outPortTypes, modelName, jobId, settings);
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
        getSettings().check(tableSpec);
        return new PortObjectSpec[]{new SparkMLModelPortObjectSpec(getSparkVersion(spec),
            getModelName(),
            getSettings().getSettings(tableSpec).getLearningTableSpec(),
            getSettings().getClassCol())};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting " + getModelName() + " model learner");
        exec.checkCanceled();
        final S settings = getSettings();
        final String newNamedModelId = UUID.randomUUID().toString();
        final MLModelLearnerJobOutput result = executeSparkJob(exec, inData, newNamedModelId, settings);

        exec.setMessage(getModelName() + " model learner done.");

        final FileStore modelFileStore = exec.createFileStore("zippedPipelineModel");
        Files.move(result.getZippedPipelineModel(), modelFileStore.getFile().toPath());

        final SparkDataPortObject data = (SparkDataPortObject)inData[0];

        final MLMetaData metaData = result.getMetaData(MLMetaData.class);

        final MLModel mlModel = new MLModel(getSparkVersion(data),
            getModelName(),
            modelFileStore.getFile(),
            newNamedModelId,
            settings.getSettings(data),
            metaData);

        return new PortObject[]{new SparkMLModelPortObject(mlModel, modelFileStore)};
    }


    @Override
    protected final ModelJobOutput executeSparkjob(final ExecutionMonitor exec, final PortObject[] inData,
        final S settings) throws KNIMESparkException, CanceledExecutionException, InvalidSettingsException {
        // do nothing
        return null;
    }

    @Override
    protected final I createJobInput(final PortObject[] inData, final S settings) throws InvalidSettingsException {
        // do nothing
        return null;
    }

    /**
     * @param exec {@link ExecutionMonitor}
     * @param inData the input {@link SparkDataPortObject}
     * @param newNamedModelId
     * @param settings the {@link MLlibSettings}
     * @return the {@link ModelJobOutput}
     * @throws KNIMESparkException indicates an exception in Spark
     * @throws CanceledExecutionException the user has canceled the execution
     * @throws InvalidSettingsException
     */
    protected MLModelLearnerJobOutput executeSparkJob(final ExecutionMonitor exec, final PortObject[] inData,
        final String newNamedModelId, final S settings)
        throws KNIMESparkException, CanceledExecutionException, InvalidSettingsException {

        final I jobInput = createJobInput(inData, newNamedModelId, settings);
        final SparkDataPortObject data = (SparkDataPortObject) inData[0];

        return SparkContextUtil.<I, MLModelLearnerJobOutput>getJobRunFactory(data.getContextID(), getJobId())
                .createRun(jobInput)
                .run(data.getContextID(), exec);
    }

    /**
     * @param inData the input {@link SparkDataPortObject}
     * @param newNamedModelId Key/ID of the named model to learn
     * @param settings {@link MLlibSettings}
     * @return the {@link JobInput}
     * @throws InvalidSettingsException if the settings are invalid
     */
    protected abstract I createJobInput(final PortObject[] inData, final String newNamedModelId, final S settings)
        throws InvalidSettingsException;

}
