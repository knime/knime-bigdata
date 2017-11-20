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
package org.knime.bigdata.spark1_5.hive;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Provides a {@link HiveContext} singleton and runs hive actions synchronized. To disable the synchronization, set
 * {@link HiveContextProvider#SYNC_CONFIG_KEY} to <code>false</code> in {@link SparkConf} of given context.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class HiveContextProvider {
    private final static Logger LOGGER = Logger.getLogger(HiveContextProvider.class);

    /** Spark context configuration key */
    public final static String SYNC_CONFIG_KEY = "spark.knime.synchronizeHiveContext";

    /** Default spark context configuration value */
    public final static boolean SYNC_CONFIG_DEFAULT = true;

    /** Internal hive context singleton */
    private static HiveContext hiveContextInstance;

    /**
     * Implement this interface to run Hive actions
     *
     * @param <T> return type of Hive action
     */
    public interface HiveContextAction<T> {
        /**
         * @param ctx Hive context to run actions
         * @return Result of Hive action
         */
        public T runWithHiveContext(final HiveContext ctx);
    }

    /**
     * Run a given action against HiveContext singleton
     * @param sc Spark context to initialize Hive context
     * @param action Action to run
     * @return Result of Hive action
     */
    public static <T> T runWithHiveContext(final SparkContext sc, final HiveContextAction<T> action) {
        final HiveContext context = getContext(sc);

        if(sc.getConf().getBoolean(SYNC_CONFIG_KEY, SYNC_CONFIG_DEFAULT)) {
            LOGGER.info("Running hive action synchronized.");
            synchronized (context) {
                return action.runWithHiveContext(context);
            }
        } else {
            LOGGER.info("Running hive action without synchronization.");
            return action.runWithHiveContext(context);
        }
    }

    private static synchronized HiveContext getContext(final SparkContext sc) {
        if (hiveContextInstance == null) {
            hiveContextInstance = new HiveContext(sc);
        }

        return hiveContextInstance;
    }
}
