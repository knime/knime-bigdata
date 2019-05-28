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
package org.knime.bigdata.spark2_4.api;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.spark.ml.PipelineModel;
import org.knime.bigdata.spark.core.model.DefaultMLModelHelper;
import org.knime.bigdata.spark.core.port.model.MLModel;
import org.knime.core.util.FileUtil;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public abstract class Spark_2_4_MLModelHelper extends DefaultMLModelHelper<PipelineModel> {

    /**
     * @param modelName the unique name of the model
     */
    public Spark_2_4_MLModelHelper(final String modelName) {
        super(modelName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PipelineModel loadModel(final MLModel model) throws IOException {
        Path unzippedModel = null;
        try {
            unzippedModel = model.unpackZippedPipelineModel();
            return PipelineModel.load(unzippedModel.toString());
        } finally {
            if (unzippedModel != null) {
                FileUtil.deleteRecursively(unzippedModel.toFile());
            }
        }
    }
}
