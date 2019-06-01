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
package org.knime.bigdata.spark.core.job;

import java.nio.file.Path;

import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;

/**
 * Job output for Spark ML model learner jobs. Instances of this class hold (1) a zip-file that contains a saved Spark
 * PipelineModel and an optional {@link MLMetaData} object, which holds nominal value mappings and model-specific
 * meta data.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLModelLearnerJobOutput extends JobOutput {

    private static final String KEY_META_DATA = "metaData";

    /**
     * Empty constructor required by deserialization.
     */
    public MLModelLearnerJobOutput() {
    }

    /**
     * Creates a job output with a MLLib model.
     *
     * @param zippedPipelineModel the ML model as a saved and then zipped pipeline model
     * @param metaData Meta data for the model. May be null.
     */
    public MLModelLearnerJobOutput(final Path zippedPipelineModel,
        final MLMetaData metaData) {

        withFile(zippedPipelineModel);

        if (metaData != null) {
            setJobData(KEY_META_DATA, metaData);
        }
    }

    /**
     * @return the generated Spark MLlib model
     */
    public Path getZippedPipelineModel() {
        return getFiles().get(0);
    }

    /**
     * @param metaDataClass
     * @return a {@link JobData} instance with metadata about the model. May be null.
     */
    public <T extends MLMetaData> T getMetaData(final Class<T> metaDataClass) {
        final T toReturn = getJobData(KEY_META_DATA, metaDataClass);
        return (toReturn.getInternalMap().isEmpty()) ? null : toReturn;
    }
}
