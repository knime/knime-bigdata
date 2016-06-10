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
package com.knime.bigdata.spark.node.mllib.prediction.bayes.naive;

import com.knime.bigdata.spark.core.job.ClassificationJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class NaiveBayesJobInput extends ClassificationJobInput {

    /**The smoothing parameter*/
    private static final String LAMBDA = "Lambda";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public NaiveBayesJobInput() {}

    /**
     * @param namedInputObject
     * @param classColIdx
     * @param featureColIdxs
     * @param lambda
     * @param namedOutputObject
     */
    public NaiveBayesJobInput(final String namedInputObject, final int classColIdx,
        final Integer[] featureColIdxs, final double lambda, final String namedOutputObject) {
        super(namedInputObject, classColIdx, featureColIdxs);
        addNamedInputObject(namedInputObject);
        set(LAMBDA, lambda);
    }

    /**
     * @return the smoothing parameter
     */
    public Double getLambda() {
        return getDouble(LAMBDA);
    }
}
