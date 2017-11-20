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
 *   Created on Sep 06, 2016 by sascha
 */
package org.knime.bigdata.spark2_1.jobs.database;

import org.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import org.knime.bigdata.spark.node.io.database.writer.Spark2DatabaseJobInput;
import org.knime.bigdata.spark.node.io.database.writer.Spark2DatabaseNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2DatabaseJobRunFactory extends DefaultJobWithFilesRunFactory<Spark2DatabaseJobInput, EmptyJobOutput> {

    /** Default constructor. */
    public Spark2DatabaseJobRunFactory() {
        super(Spark2DatabaseNodeModel.JOB_ID, Spark2DatabaseJob.class, EmptyJobOutput.class, FileLifetime.CONTEXT,
            true);

    }
}
