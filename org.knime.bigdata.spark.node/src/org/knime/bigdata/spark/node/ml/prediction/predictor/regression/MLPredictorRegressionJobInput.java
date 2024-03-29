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
package org.knime.bigdata.spark.node.ml.prediction.predictor.regression;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLPredictorRegressionJobInput extends JobInput {

    private static final String KEY_PREDICTION_COLUMN = "predictionColumn";

    private static final String KEY_NAMED_MODEL_ID = "namedModelId";

    /**
     * Empty constructor for (de)serialization.
     */
    public MLPredictorRegressionJobInput() {
    }

    /**
     * Constructor.
     *
     * @param namedInputObject Key/ID of the dataset to use for prediction.
     * @param namedModelId Key/ID of the named model to use for prediction.
     * @param namedOutputObject Key/ID of the dataset to create.
     * @param predictionColumn Name of the prediction column to append.
     */
    public MLPredictorRegressionJobInput(final String namedInputObject, final String namedModelId,
        final String namedOutputObject, final String predictionColumn) {

        addNamedInputObject(namedInputObject);
        set(KEY_PREDICTION_COLUMN, predictionColumn);
        set(KEY_NAMED_MODEL_ID, namedModelId);
        addNamedOutputObject(namedOutputObject);
    }

    /**
     * @return the key/ID of the named model to use for prediction.
     */
    public String getNamedModelId() {
        return get(KEY_NAMED_MODEL_ID);
    }

    /**
     * @return the desired column name for the prediction column to append.
     */
    public String getPredictionColumn() {
        return get(KEY_PREDICTION_COLUMN);
    }
}
