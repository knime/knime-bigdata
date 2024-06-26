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
 *   Created on Apr 15, 2016 by bjoern
 */
package org.knime.bigdata.spark3_5.jobs.table2spark;

import org.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import org.knime.bigdata.spark.node.io.table.reader.Table2SparkJobInput;
import org.knime.bigdata.spark.node.io.table.reader.Table2SparkNodeModel;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class Table2SparkJobRunFactory extends DefaultJobWithFilesRunFactory<Table2SparkJobInput, EmptyJobOutput> {

    /**
     * Constructor.
     */
    public Table2SparkJobRunFactory() {
        super(Table2SparkNodeModel.JOB_ID, Table2SparkJob.class, EmptyJobOutput.class, FileLifetime.JOB, false);
    }
}
