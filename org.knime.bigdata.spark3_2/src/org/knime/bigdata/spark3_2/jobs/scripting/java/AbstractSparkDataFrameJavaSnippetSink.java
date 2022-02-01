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
 *   Created on 24.06.2015 by koetter
 */
package org.knime.bigdata.spark3_2.jobs.scripting.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public abstract class AbstractSparkDataFrameJavaSnippetSink extends AbstractSparkDataFrameJavaSnippet {

    private static final long serialVersionUID = 1843886386294265404L;

    @Override
    public Dataset<Row> apply(final SparkSession spark, final Dataset<Row> dataFrame1, final Dataset<Row> dataFrame2) throws Exception {
        apply(spark, dataFrame1);
        return null;
    }

    /**
     * @param spark the SparkSession
     * @param dataFrame the input data frame
     * @throws Exception if an exception occurs
     */
    public abstract void apply(final SparkSession spark, final Dataset<Row> dataFrame) throws Exception;
}
