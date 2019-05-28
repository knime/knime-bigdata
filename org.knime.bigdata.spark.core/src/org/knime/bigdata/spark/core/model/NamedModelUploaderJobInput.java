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

import java.nio.file.Path;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class NamedModelUploaderJobInput extends JobInput {

    private static final String KEY_NAMED_MODEL_ID = "namedModelId";

    /**
     * Empty constructor for (de)serialization.
     */
    public NamedModelUploaderJobInput() {
    }

    /**
     * Creates a new instance.
     *
     * @param namedModelId Key/ID of the named model.
     * @param zippedPipelineModel A file that contains the zipped pipeline model.
     */
    public NamedModelUploaderJobInput(final String namedModelId, final Path zippedPipelineModel) {
        set(KEY_NAMED_MODEL_ID, namedModelId);
        withFile(zippedPipelineModel);
    }

    /**
     * @return The Key/ID of the named model.
     */
    public String getNamedModelId() {
        return get(KEY_NAMED_MODEL_ID);
    }

    /**
     * @return a file that contains the zipped pipeline model.
     */
    public Path getZippedModelPipeline() {
        return getFiles().get(0);
    }
}
