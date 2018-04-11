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
 *   Created on Feb 11, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark2_1.jobs.mllib.freqitemset;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrequentItemSetNodeModel;
import org.knime.bigdata.spark2_1.api.Spark_2_1_ModelHelper;

/**
 * Frequent item sets model helper.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FrequentItemSetModelHelper extends Spark_2_1_ModelHelper {

    /** Default constructor. */
    public FrequentItemSetModelHelper() {
        super(SparkFrequentItemSetNodeModel.MODEL_NAME);
    }

    @Override
    public ModelInterpreter getModelInterpreter() {
        return FrequentItemSetModelInterpreter.getInstance();
    }

    @Override
    public Serializable loadMetaData(final InputStream inputStream) throws IOException {
        return loadModel(inputStream);
    }

    @Override
    public void saveModelMetadata(final OutputStream outputStream, final Serializable modelMetadata) throws IOException {
        saveModel(outputStream, modelMetadata);
    }
}
