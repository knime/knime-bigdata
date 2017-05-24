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
 */
package com.knime.bigdata.spark2_0.jobs.scripting.java;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * @author Tobias Koetter, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public abstract class AbstractSparkJavaSnippetSource extends AbstractSparkJavaSnippet {

    private static final long serialVersionUID = 4248837360910849154L;

    @Override
    public Dataset<Row> apply(final JavaSparkContext sc, final Dataset<Row> dataFrame1, final Dataset<Row> dataFrame2) throws Exception {
        return apply(sc);
    }

    /**
     * @param sc the JavaSparkContext
     * @return the resulting data frame or <code>null</code>
     * @throws Exception if an exception occurs
     */
    public abstract Dataset<Row> apply(final JavaSparkContext sc) throws Exception;
}
