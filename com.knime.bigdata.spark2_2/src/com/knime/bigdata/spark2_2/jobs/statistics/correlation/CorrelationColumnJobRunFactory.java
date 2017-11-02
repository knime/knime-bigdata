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
package com.knime.bigdata.spark2_2.jobs.statistics.correlation;

import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.node.statistics.correlation.CorrelationColumnJobOutput;
import com.knime.bigdata.spark.node.statistics.correlation.CorrelationJobInput;
import com.knime.bigdata.spark.node.statistics.correlation.column.MLlibCorrelationColumnNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class CorrelationColumnJobRunFactory
    extends DefaultJobRunFactory<CorrelationJobInput, CorrelationColumnJobOutput> {

    /**
     * Constructor.
     */
    public CorrelationColumnJobRunFactory() {
        super(MLlibCorrelationColumnNodeModel.JOB_ID, CorrelationColumnJob.class, CorrelationColumnJobOutput.class);
    }
}
