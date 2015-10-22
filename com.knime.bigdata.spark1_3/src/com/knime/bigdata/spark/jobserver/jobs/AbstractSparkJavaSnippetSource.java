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
package com.knime.bigdata.spark.jobserver.jobs;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkJavaSnippetSource extends AbstractSparkJavaSnippet {

    private final static Logger LOGGER = Logger.getLogger(AbstractSparkJavaSnippetSource.class.getName());
    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public JavaRDD<Row> apply(final JavaSparkContext sc, final JavaRDD<Row> rowRDD1, final JavaRDD<Row> rowRDD2)
            throws GenericKnimeSparkException {
        LOGGER.log(Level.FINE, "starting apply...");
        final JavaRDD<Row> result = apply(sc);
        LOGGER.log(Level.FINE, "apply finished...");
        return result;
    }

    /**
     * @param sc the JavaSparkContext
     * @return the result rdd
     * @throws GenericKnimeSparkException if an exception occurs
     */
    public abstract JavaRDD<Row> apply(JavaSparkContext sc) throws GenericKnimeSparkException;

}
