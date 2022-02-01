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
 */
package org.knime.bigdata.spark3_2.jobs.sql;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.core.job.EmptyJobInput;
import org.knime.bigdata.spark.node.sql.SparkSQLFunctionsJobOutput;
import org.knime.bigdata.spark.node.sql.SparkSQLNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class SparkSQLFunctionsJobRunFactory extends DefaultJobRunFactory<EmptyJobInput, SparkSQLFunctionsJobOutput> {

    /** Default constructor. */
    public SparkSQLFunctionsJobRunFactory() {
        super(SparkSQLNodeModel.FUNCTIONS_JOB_ID, SparkSQLFunctionsJob.class, SparkSQLFunctionsJobOutput.class);
    }
}
