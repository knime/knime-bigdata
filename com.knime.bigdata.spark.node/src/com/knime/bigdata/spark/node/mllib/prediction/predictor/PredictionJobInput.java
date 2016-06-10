/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.predictor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PredictionJobInput extends JobInput {

    private static final String KEY_MODEL = "model";
    private final static String KEY_INCLUDE_COLUMN_INDICES = "includeColIdxs";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public PredictionJobInput() {
    }

    /**
     * @param namedInputObject the unique name of the input object
     * @param model the MLlib model to use for prediction
     * @param colIdxs the column indices to us for prediction
     * @param namedOutputObject the unique name of the output object
     */
    public PredictionJobInput(final String namedInputObject, final Serializable model,
        final Integer[] colIdxs, final String namedOutputObject) {
        if (model == null) {
            throw new IllegalArgumentException("Prediction model must not be null");
        }
        if (colIdxs == null || colIdxs.length < 1) {
            throw new IllegalArgumentException("Prediction columns must not be null or empty");
        }
        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(KEY_MODEL, model);
        set(KEY_INCLUDE_COLUMN_INDICES, Arrays.asList(colIdxs));
    }


    /**
     * @return the indices of the columns to use for prediction
     */
    public List<Integer> getIncludeColumnIndices() {
        return get(KEY_INCLUDE_COLUMN_INDICES);
    }

    /**
     * @return the Spark MLlib model
     */
    public Serializable getModel() {
        return get(KEY_MODEL);
    }

}
