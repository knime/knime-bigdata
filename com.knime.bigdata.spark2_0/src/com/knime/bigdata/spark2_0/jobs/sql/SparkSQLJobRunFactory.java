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
 */
package com.knime.bigdata.spark2_0.jobs.sql;

import com.knime.bigdata.spark.core.job.DefaultJobRun;
import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.node.sql.SparkSQLJobInput;
import com.knime.bigdata.spark.node.sql.SparkSQLJobOutput;
import com.knime.bigdata.spark.node.sql.SparkSQLNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class SparkSQLJobRunFactory extends DefaultJobRunFactory<SparkSQLJobInput, SparkSQLJobOutput> {

    /** Default constructor. */
    public SparkSQLJobRunFactory() {
        super(SparkSQLNodeModel.JOB_ID);
    }

    @Override
    public JobRun<SparkSQLJobInput, SparkSQLJobOutput> createRun(final SparkSQLJobInput input) {
        return new DefaultJobRun<>(input, SparkSQLJob.class, SparkSQLJobOutput.class);
    }
}