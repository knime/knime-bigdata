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
package org.knime.bigdata.spark1_5.jobs.statistics.correlation;

import org.apache.spark.mllib.linalg.Matrix;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.statistics.correlation.CorrelationColumnJobOutput;

/**
 * Computes a single correlation value
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class CorrelationColumnJob extends CorrelationJob<CorrelationColumnJobOutput> {

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    protected CorrelationColumnJobOutput createJobOutput(final Matrix mat) {
        final double correlation = mat.apply(0, 1);
        return new CorrelationColumnJobOutput(correlation);
    }
}
