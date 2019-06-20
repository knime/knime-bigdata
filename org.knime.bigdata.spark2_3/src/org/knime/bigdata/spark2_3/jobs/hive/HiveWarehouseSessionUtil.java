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
 *   Created on Apr 11, 2019 by bjoern
 */
package org.knime.bigdata.spark2_3.jobs.hive;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobInput;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;

import com.hortonworks.hwc.HiveWarehouseSession;

/**
 * Utility class to read/write DataFrames from/to Hive using the (Hortonworks-specific) Hive Warehouse Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class HiveWarehouseSessionUtil {

    private static HiveWarehouseSession sessionInstance;

    private static synchronized HiveWarehouseSession getOrCreateSession(final SparkContext sparkContext) {
        if (sessionInstance == null) {
            sessionInstance =
                HiveWarehouseSession.session(SparkSession.builder().sparkContext(sparkContext).getOrCreate()).build();
        }
        return sessionInstance;
    }

    /**
     * Executes the query in the given job input using Hive Warehouse Connector and returns the result as a data frame.
     *
     * @param sparkContext The surrounding Spark context-
     * @param input Job input with the query.
     * @return the result as a data frame.
     */
    public static Dataset<Row> queryHive(final SparkContext sparkContext, final Hive2SparkJobInput input) {
        final HiveWarehouseSession session = getOrCreateSession(sparkContext);
        return session.executeQuery(input.getQuery());
    }

    /**
     * Writes the given data frame as a Hive table using Hive Warehouse Connector. Fails if the table already exists in
     * hive.
     *
     * @param sparkContext The surrounding Spark context.
     * @param dataFrame The data frame to write.
     * @param input Job input with
     * @throws KNIMESparkException
     */
    public static void writeToHive(final SparkContext sparkContext, final Dataset<Row> dataFrame,
        final Spark2HiveJobInput input) throws KNIMESparkException {

        if (FileFormat.valueOf(input.getFileFormat()) != FileFormat.ORC
                && FileFormat.valueOf(input.getFileFormat()) != FileFormat.CLUSTER_DEFAULT) {

            throw new KNIMESparkException(String.format(
                "Hive Warehouse Connector does not support writing tables in %s format, only ORC is currently supported.",
                input.getFileFormat()));
        }

        // NOTE: Always make sure that HiveWarehouseSession has been initialized before doing this
        getOrCreateSession(sparkContext);

        // Table was already dropped in the KNIME node model and should not exist.
        dataFrame.write().format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR).mode("error")
            .option("table", input.getHiveTableName()).save();
    }
}
