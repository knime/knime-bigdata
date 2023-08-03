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
package org.knime.bigdata.spark3_4.jobs.genericdatasource;

import org.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2GenericDataSourceJobRunFactory
    extends DefaultJobWithFilesRunFactory<Spark2GenericDataSourceJobInput, EmptyJobOutput> {

    /** Constructor */
    public Spark2GenericDataSourceJobRunFactory() {
        super(Spark2GenericDataSourceNodeModel.JOB_ID, Spark2GenericDataSourceJob.class, EmptyJobOutput.class,
            FileLifetime.CONTEXT, true);
    }
}
