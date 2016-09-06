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
package com.knime.bigdata.spark1_5.jobs.parquet;

import com.knime.bigdata.spark.core.job.DefaultSimpleJobRun;
import com.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import com.knime.bigdata.spark.core.job.SimpleJobRun;
import com.knime.bigdata.spark.node.io.parquet.writer.Spark2ParquetJobInput;
import com.knime.bigdata.spark.node.io.parquet.writer.Spark2ParquetNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2ParquetJobRunFactory extends DefaultSimpleJobRunFactory<Spark2ParquetJobInput> {

    /** Constructor */
    public Spark2ParquetJobRunFactory() {
        super(Spark2ParquetNodeModel.JOB_ID);
    }

    @Override
    public SimpleJobRun<Spark2ParquetJobInput> createRun(final Spark2ParquetJobInput input) {
        return new DefaultSimpleJobRun<>(input, Spark2ParquetJob.class);
    }
}
