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
package org.knime.bigdata.spark2_2.jobs.scripting.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public abstract class AbstractSparkJavaSnippetSource extends AbstractSparkJavaSnippet {

    private static final long serialVersionUID = 4248837360910849154L;

    /**
     * {@inheritDoc}
     */
    @Override
    public JavaRDD<Row> apply(final JavaSparkContext sc, final JavaRDD<Row> rowRDD1, final JavaRDD<Row> rowRDD2)
        throws Exception {

        return apply(sc);
    }

    /**
     * @param sc the JavaSparkContext
     * @return the result rdd
     * @throws Exception if an exception occurs
     */
    public abstract JavaRDD<Row> apply(JavaSparkContext sc) throws Exception;

}
