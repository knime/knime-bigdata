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

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLModelLearnerJobOutput extends JobOutput {

    /**
     * Empty constructor required by deserialization.
     */
    public MLModelLearnerJobOutput() {
    }

    /**
     * Creates a job output with a MLLib model.
     *
     * @param zippedPipelineModel the ML model as a saved and then zipped pipeline model
     */
    public MLModelLearnerJobOutput(final Path zippedPipelineModel) {
        withFile(zippedPipelineModel);
    }

    /**
     * @return the generated Spark MLlib model
     */
    public Path getZippedPipelineModel() {
        return getFiles().get(0);
    }
}
