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
 *   Created on 29.01.2016 by koetter
 */
package org.knime.bigdata.spark3_3.jobs.statistics.correlation;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.core.job.HalfDoubleMatrixJobOutput;
import org.knime.bigdata.spark.node.statistics.correlation.CorrelationJobInput;
import org.knime.bigdata.spark.node.statistics.correlation.matrix.MLlibCorrelationMatrixNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class CorrelationMatrixJobRunFactory
    extends DefaultJobRunFactory<CorrelationJobInput, HalfDoubleMatrixJobOutput> {

    /**
     * Constructor.
     */
    public CorrelationMatrixJobRunFactory() {
        super(MLlibCorrelationMatrixNodeModel.JOB_ID, CorrelationMatrixJob.class, HalfDoubleMatrixJobOutput.class);
    }
}
