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
 */
package org.knime.bigdata.spark.node.scorer;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Scorer job input.
 *
 * @author dwk
 */
@SparkClass
public class ScorerJobInput extends JobInput {

    private static final String REF_COL_IDX = "referenceColIdx";
    private static final String PREDICTION_COL_IDX = "predictionColIdx";
    private static final String FAIL_ON_MISSING_VALUES = "failOnMissingValues";


    /**
     * Paramless constructor for automatic deserialization.
     */
    public ScorerJobInput() {}

    /**
     * @param namedInputObject input object
     * @param refColIdx reference column index
     * @param predictionColIdx prediction column index
     * @param failOnMissingValues <code>true</code> if the job should fail on missing values
     */
    public ScorerJobInput(final String namedInputObject, final Integer refColIdx, final Integer predictionColIdx, final boolean failOnMissingValues) {
        addNamedInputObject(namedInputObject);
        set(REF_COL_IDX, refColIdx);
        set(PREDICTION_COL_IDX, predictionColIdx);

        if (failOnMissingValues) {
            set(FAIL_ON_MISSING_VALUES, true);
        }
    }

    /**
     * @return the index of the reference value column
     */
    public Integer getRefColIdx() {
        return getInteger(REF_COL_IDX);
    }

    /**
     * @return the index of the prediction column
     */
    public Integer getPredictionColIdx() {
        return getInteger(PREDICTION_COL_IDX);
    }

    /**
     * @return <code>true</code> if job should fail on missing values in the reference or prediction column
     */
    public boolean failOnMissingValues() {
        return has(FAIL_ON_MISSING_VALUES);
    }
}
