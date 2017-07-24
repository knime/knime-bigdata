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
 *   Created on 24.06.2015 by koetter
 */
package com.knime.bigdata.spark2_1.jobs.scripting.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public abstract class AbstractSparkDataFrameJavaSnippetSource extends AbstractSparkDataFrameJavaSnippet {

    private static final long serialVersionUID = 4248837360910849154L;

    @Override
    public Dataset<Row> apply(final SparkSession spark, final Dataset<Row> dataFrame1, final Dataset<Row> dataFrame2) throws Exception {
        return apply(spark);
    }

    /**
     * @param spark the SparkSession
     * @return the result data frame
     * @throws Exception if an exception occurs
     */
    public abstract Dataset<Row> apply(final SparkSession spark) throws Exception;
}
