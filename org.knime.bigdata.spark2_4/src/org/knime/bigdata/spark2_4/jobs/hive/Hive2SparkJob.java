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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark2_4.jobs.hive;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobInput;
import org.knime.bigdata.spark2_4.api.NamedObjects;
import org.knime.bigdata.spark2_4.api.SimpleSparkJob;

/**
 * Executes given SQL statement and puts result into a (named) data frame.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class Hive2SparkJob implements SimpleSparkJob<Hive2SparkJobInput> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(Hive2SparkJob.class.getName());

    /**
     * SparkConf setting to switch the usage of Hive Warehouse Connector to on/off/auto.
     */
    public static final String USE_HIVE_WAREHOUSE_CONNECTOR = "spark.knime.useHiveWarehouseConnector";

    @Override
    public void runJob(final SparkContext sparkContext, final Hive2SparkJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        final Dataset<Row> dataFrame;

        if (shouldUseHiveWarehouseConnector(sparkContext)) {
            dataFrame = HiveWarehouseSessionUtil.queryHive(sparkContext, input);
        } else {
            dataFrame = runQueryInHiveSession(sparkContext, input);
        }

        for (final StructField field : dataFrame.schema().fields()) {
            LOGGER.debug("Field '" + field.name() + "' of type '" + field.dataType() + "'");
        }

        final String key = input.getFirstNamedOutputObject();
        LOGGER.info("Storing Hive query result under key: " + key);
        namedObjects.addDataFrame(key, dataFrame);
    }

    @SuppressWarnings("resource")
    private static Dataset<Row> runQueryInHiveSession(final SparkContext sparkContext, final Hive2SparkJobInput input)
        throws KNIMESparkException {

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        ensureHiveSupport(spark);
        return spark.sql(input.getQuery());
    }

    private static void ensureHiveSupport(final SparkSession spark) throws KNIMESparkException {
        if (!spark.conf().get("spark.sql.catalogImplementation", "in-memory").equals("hive")) {
            throw new KNIMESparkException("Spark session does not support hive!"
                + " Please set spark.sql.catalogImplementation = \"hive\".");
        }
    }

    /**
     * Checks SparConf, whether Hortonwork's Hive Warehouse Connector shall be used for the Hive2Spark and Spark2Hive
     * jobs.
     *
     * @param sparkContext The current Spark context.
     * @return true, if Hive Warehouse Connector shall be used, false otherwise.
     */
    public static boolean shouldUseHiveWarehouseConnector(final SparkContext sparkContext) {
        final String useHwc = sparkContext.conf().get(USE_HIVE_WAREHOUSE_CONNECTOR, "auto");

        if (useHwc.equalsIgnoreCase("auto")) {
            if (sparkContext.conf().contains("spark.datasource.hive.warehouse.metastoreUri")) {
                return true;
            } else {
                return false;
            }
        } else if (useHwc.equalsIgnoreCase("true") || useHwc.equalsIgnoreCase("on") || useHwc.equalsIgnoreCase("yes")) {
            return true;
        } else if (useHwc.equalsIgnoreCase("false") || useHwc.equalsIgnoreCase("off")
            || useHwc.equalsIgnoreCase("no")) {
            return false;
        } else {
            LOGGER.warn(String.format("Invalid SparkConf setting %s=%s", USE_HIVE_WAREHOUSE_CONNECTOR, useHwc));
            return false;
        }
    }
}
