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
 *   Created on Aug 11, 2016 by sascha
 */
package com.knime.bigdata.spark1_2.jobs.parquet;

import com.knime.bigdata.spark.core.job.DefaultJobRun;
import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.node.io.parquet.reader.Parquet2SparkJobInput;
import com.knime.bigdata.spark.node.io.parquet.reader.Parquet2SparkJobOutput;
import com.knime.bigdata.spark.node.io.parquet.reader.Parquet2SparkNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Parquet2SparkJobRunFactory extends DefaultJobRunFactory<Parquet2SparkJobInput, Parquet2SparkJobOutput> {

    /** Constructor */
    public Parquet2SparkJobRunFactory() {
        super(Parquet2SparkNodeModel.JOB_ID);
    }

    @Override
    public JobRun<Parquet2SparkJobInput, Parquet2SparkJobOutput> createRun(final Parquet2SparkJobInput input) {
        return new DefaultJobRun<>(input, Parquet2SparkJob.class, Parquet2SparkJobOutput.class);
    }
}
