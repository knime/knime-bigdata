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
package com.knime.bigdata.spark.node.mllib.reduction.svd;

import java.util.List;

import com.knime.bigdata.spark.core.job.ColumnsJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class SVDJobInput extends ColumnsJobInput {

    /**
     * the reciprocal condition number. All singular values smaller than rCond * sigma(0) are treated as zero, where
     * sigma(0) is the largest singular value.
     */
    private static final String RCOND = "RCond";

    /**
     * number of leading singular values to keep (0 < k <= n). It might return less than k if there are numerically zero
     * singular values or there are not enough Ritz values converged before the maximum number of Arnoldi update
     * iterations is reached (in case that matrix A is ill-conditioned).
     */
    private static final String K = "k";

    /**
     * whether to compute U
     */
    private static final String COMPUTE_U = "computeU";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public SVDJobInput() {
    }

    SVDJobInput(final String namedInputObject, final Integer[] featureColIdxs, final boolean aComputeU, final int aK,
        final double aRCond, final String aVMatrix, final String aUMatrix) {
        super(namedInputObject, featureColIdxs);
        set(K, aK);
        set(COMPUTE_U, aComputeU);
        set(RCOND, aRCond);
        //named RDD with V matrix
        addNamedOutputObject(aVMatrix);
        if (aComputeU) {
            //named RDD with U matrix (iff computeU == true)
            addNamedOutputObject(aUMatrix);
        }
    }

    /**
     * @return number of leading singular values to keep (0 < k <= n). It might return less than k if there are
     *         numerically zero singular values or there are not enough Ritz values converged before the maximum number
     *         of Arnoldi update iterations is reached (in case that matrix A is ill-conditioned).
     */
    public int getK() {
        return getInteger(K);
    }

    /**
     * @return the reciprocal condition number. All singular values smaller than rCond * sigma(0) are treated as zero,
     *         where sigma(0) is the largest singular value.
     */
    public double getRCond() {
        return getDouble(RCOND);
    }

    /**
     * @return <code>true</code> if u matrix should be computed and returned
     */
    public boolean computeU() {
        return get(COMPUTE_U);
    }

    /**
     * @return the name of the U matrix to use or <code>null</code> if u should not be computed
     * @see #computeU()
     */
    public String getUMatrixName() {
        List<String> outputObjects = getNamedOutputObjects();
        if (computeU() && outputObjects.size() == 2) {
            return outputObjects.get(1);
        }
        return null;
    }

    /**
     * @return the V matrix name to use
     */
    public String getVMatrixName() {
        return getFirstNamedOutputObject();
    }
}
