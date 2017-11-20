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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.reduction.pca;

import com.knime.bigdata.spark.core.job.ColumnsJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PCAJobInput extends ColumnsJobInput {

    /**
     * number of top principal components.
     */
    private static final String K = "k";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public PCAJobInput(){}

    PCAJobInput(final String namedInputObject, final Integer[] featureColIdxs, final int k,
        final String matrix, final String projectionMatrixName) {
        super(namedInputObject, featureColIdxs);
        addNamedOutputObject(matrix);
        addNamedOutputObject(projectionMatrixName);
        set(K, k);
    }

    /**
     * @return number of top principal components.
     */
    public int getK() {
        return getInteger(K);
    }

    /**
     * @return the matrix name to use
     */
    public String getMatrixName() {
        return getNamedOutputObjects().get(0);
    }

    /**
     * @return the projection matrix name
     */
    public String getProjectionMatrix() {
        return getNamedOutputObjects().get(1);
    }
}
