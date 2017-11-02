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
 *   Created on Aug 11, 2016 by sascha
 */
package com.knime.bigdata.spark2_1.jobs.genericdatasource;

import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobInput;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobOutput;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class GenericDataSource2SparkJobRunFactory
    extends DefaultJobWithFilesRunFactory<GenericDataSource2SparkJobInput, GenericDataSource2SparkJobOutput> {

    /** Constructor */
    public GenericDataSource2SparkJobRunFactory() {
        super(GenericDataSource2SparkNodeModel.JOB_ID, GenericDataSource2SparkJob.class,
            GenericDataSource2SparkJobOutput.class, FileLifetime.CONTEXT, true);
    }
}
