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
package com.knime.bigdata.spark.node.pmml.predictor;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.pmml.PMMLAssignJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PMMLPredictionJobInput extends PMMLAssignJobInput {
    private static final String PROBABILITIES = "appendProbabilities";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public PMMLPredictionJobInput() {}

    /**
     * Constructor.
     * @param inputID
     * @param colIdxs
     * @param mainClass
     * @param outputID
     * @param appendProbabilities <code>true</code> if probability columns should be added
     */
    public PMMLPredictionJobInput(final String inputID, final Integer[] colIdxs, final String mainClass,
        final String outputID, final boolean appendProbabilities) {
        super(inputID, colIdxs, mainClass, outputID);
        set(PROBABILITIES, appendProbabilities);
    }

    /**
     * @return <code>true</code> if the class probabilities should be appended
     */
    public boolean appendProbabilities() {
        return get(PROBABILITIES);
    }
}