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
package com.knime.bigdata.spark1_2.hive;

import java.lang.reflect.InvocationTargetException;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * Provides a {@link JavaHiveContext} and runs hive actions on it. This is a dummy implementation compared to other
 * spark versions and does not use a singleton (creating a singleton instance of {@link JavaHiveContext} results in
 * {@link InvocationTargetException}).
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class HiveContextProvider {

    /**
     * Implement this interface to run Hive actions
     *
     * @param <T> return type of Hive action
     */
    @SparkClass
    public interface HiveContextAction<T> {
        /**
         * @param ctx Hive context to run actions
         * @return Result of Hive action
         */
        public T runWithHiveContext(final JavaHiveContext ctx);
    }

    /**
     * Run a given action against HiveContext
     * @param sc Spark context to initialize Hive context
     * @param action Action to run
     * @return Result of Hive action
     */
    public static <T> T runWithHiveContext(final SparkContext sc, final HiveContextAction<T> action) {
        final JavaHiveContext hiveContext = new JavaHiveContext(JavaSparkContext.fromSparkContext(sc));
        return action.runWithHiveContext(hiveContext);
    }
}
