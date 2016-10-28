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
 *   Created on Sep 06, 2016 by sascha
 */
package com.knime.bigdata.spark1_5.jobs.database;

import java.io.File;
import java.util.List;

import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRun;
import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.JobWithFilesRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import com.knime.bigdata.spark.node.io.database.reader.Database2SparkJobInput;
import com.knime.bigdata.spark.node.io.database.reader.Database2SparkJobOutput;
import com.knime.bigdata.spark.node.io.database.reader.Database2SparkNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Database2SparkJobRunFactory extends DefaultJobWithFilesRunFactory<Database2SparkJobInput, Database2SparkJobOutput> {

    /** Default constructor. */
    public Database2SparkJobRunFactory() {
        super(Database2SparkNodeModel.JOB_ID);
    }

    @Override
    public JobWithFilesRun<Database2SparkJobInput, Database2SparkJobOutput> createRun(final Database2SparkJobInput input, final List<File> jarFiles) {
        return new DefaultJobWithFilesRun<Database2SparkJobInput, Database2SparkJobOutput>(
                input, Database2SparkJob.class, Database2SparkJobOutput.class,
                jarFiles, FileLifetime.CONTEXT, true);
    }
}
