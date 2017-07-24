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
package com.knime.bigdata.spark2_2.jobs.statistics.correlation;

import org.apache.spark.mllib.linalg.Matrix;

import com.knime.bigdata.spark.core.job.HalfDoubleMatrixJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * Computes a single correlation value
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class CorrelationMatrixJob extends CorrelationJob<HalfDoubleMatrixJobOutput> {

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    protected HalfDoubleMatrixJobOutput createJobOutput(final Matrix mat) {
        final HalfDoubleMatrixFromLinAlgMatrix matrix = new HalfDoubleMatrixFromLinAlgMatrix(mat, false);
        return matrix.getJobOutput();
    }
}
