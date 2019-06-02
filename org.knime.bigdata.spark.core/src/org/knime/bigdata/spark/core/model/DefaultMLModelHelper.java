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
 *   Created on May 27, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

/**
 *
 * @author Bjoern Lohrmann
 * @param <PIPELINEMODEL> Pipeline model type in Spark.
 */
public abstract class DefaultMLModelHelper<PIPELINEMODEL> implements MLModelHelper<PIPELINEMODEL> {

    /**
     * Job ID of the job that checks for the presence of a named model.
     */
    public final static String NAMED_MODEL_CHECKER_JOB_ID = "NamedModelCheckerJob";

    /**
     * Job ID of the job that uploads a named model and registers it.
     */
    public static final String NAMED_MODEL_UPLOADER_JOB_ID = "NamedModelUploaderJob";

    private final String m_modelName;

    /**
     * @param modelName the unique name of the model
     */
    protected DefaultMLModelHelper(final String modelName) {
        m_modelName = modelName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return m_modelName;
    }

    @Override
    public Serializable loadMetaData(final InputStream inputStream) throws IOException {
        // default implementation that does nothing
        return null;
    }

    @Override
    public void saveModelMetadata(final OutputStream outputStream, final Serializable modelMetadata)
        throws IOException {
        // default implementation that does nothing
    }

    /**
     * {@inheritDoc}
     *
     * @throws CanceledExecutionException
     */
    @Override
    public void uploadModelToSparkIfNecessary(final SparkContextID sparkContext, final MLModel model,
        final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {

        final NamedModelCheckerJobInput modelCheckerJobInput = new NamedModelCheckerJobInput(model.getNamedModelId());
        try {

            SparkContextUtil.getSimpleRunFactory(sparkContext, NAMED_MODEL_CHECKER_JOB_ID)
                .createRun(modelCheckerJobInput).run(sparkContext, exec);

            // if this does not throw any exception, then all is good (model is present on the Spark side)

        } catch (MissingNamedModelException e) {
            final NamedModelUploaderJobInput uploaderJobInput =
                new NamedModelUploaderJobInput(model.getNamedModelId(), model.getZippedPipelineModel().toPath());

            SparkContextUtil.getSimpleRunFactory(sparkContext, NAMED_MODEL_UPLOADER_JOB_ID).createRun(uploaderJobInput)
                .run(sparkContext, exec);
        }
    }
}
