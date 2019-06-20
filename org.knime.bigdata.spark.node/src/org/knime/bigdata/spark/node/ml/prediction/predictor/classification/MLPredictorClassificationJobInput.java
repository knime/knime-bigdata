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
package org.knime.bigdata.spark.node.ml.prediction.predictor.classification;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLPredictorClassificationJobInput extends JobInput {

    private static final String KEY_ORIGINAL_TARGET_COLUMN = "originalTargetColumn";

    private static final String KEY_PREDICTION_COLUMN = "predictionColumn";

    private static final String KEY_NAMED_MODEL_ID = "namedModelId";

    private static final String KEY_APPEND_PROBABILITY_COLUMNS = "appendProbabilityColumns";

    private static final String KEY_PROBABILITY_COLUMN_SUFFIX = "probabilityColumnSuffix";

    /**
     * Empty constructor for (de)serialization.
     */
    public MLPredictorClassificationJobInput() {
    }

    /**
     * Constructor.
     *
     * @param namedInputObject Key/ID of the dataset to use for prediction.
     * @param namedModelId Key/ID of the named model to use for prediction.
     * @param namedOutputObject Key/ID of the dataset to create.
     * @param originalTargetColumn The name of the original target column that the model was trained on.
     * @param predictionColumn Name of the prediction column to append.
     * @param appendProbabilityColumns
     * @param probabilityColumnSuffix the desired suffix for the probability columns.
     */
    public MLPredictorClassificationJobInput(final String namedInputObject, final String namedModelId,
        final String namedOutputObject, final String originalTargetColumn, final String predictionColumn, final boolean appendProbabilityColumns,
        final String probabilityColumnSuffix) {

        addNamedInputObject(namedInputObject);
        set(KEY_ORIGINAL_TARGET_COLUMN, originalTargetColumn);
        set(KEY_PREDICTION_COLUMN, predictionColumn);
        set(KEY_NAMED_MODEL_ID, namedModelId);
        set(KEY_APPEND_PROBABILITY_COLUMNS, appendProbabilityColumns);
        set(KEY_PROBABILITY_COLUMN_SUFFIX, probabilityColumnSuffix);
        addNamedOutputObject(namedOutputObject);
    }

    /**
     * @return the key/ID of the named model to use for prediction.
     */
    public String getNamedModelId() {
        return get(KEY_NAMED_MODEL_ID);
    }

    /**
     * @return whether to append probability columns or not.
     */
    public boolean appendProbabilityColumns() {
        return get(KEY_APPEND_PROBABILITY_COLUMNS);
    }

    /**
     * @return the desired suffix for the probability columns.
     */
    public String getProbabilityColumnSuffix() {
        return get(KEY_PROBABILITY_COLUMN_SUFFIX);
    }

    /**
     * @return the name of the original target column that the model was trained on.
     */
    public String getOriginalTargetColumn() {
        return get(KEY_ORIGINAL_TARGET_COLUMN);
    }

    /**
     * @return the desired column name for the prediction column.
     */
    public String getPredictionColumn() {
        return get(KEY_PREDICTION_COLUMN);
    }

}
