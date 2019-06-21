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
package org.knime.bigdata.spark2_4.jobs.hive;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobInput;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;

import com.hortonworks.hwc.CreateTableBuilder;
import com.hortonworks.hwc.HiveWarehouseSession;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;

/**
 * Utility class to read/write DataFrames from/to Hive using the (Hortonworks-specific) Hive Warehouse Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class HiveWarehouseSessionUtil {

    /**
     * This constant is supposed to be part of HWC but was made non-public as of HDP 3.1.2 (for whatever reason, no
     * explanation was provided).
     */
    private static final String HIVE_WAREHOUSE_CONNECTOR = "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector";

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
     * Writes the given data frame as a Hive table using Hive Warehouse Connector.
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

        // we are currently building our own CREATE TABLE statement to be able to quote column
        // names and support compression formats. As HWC becomes more featureful this may not be
        // necessary anymore.
        final String createTableStmt = buildCreateTableStatement(sparkContext, dataFrame, input);

        boolean clearSession = false;
        try {
            // we need to ensure SparkSession.getActiveSession() returns a SparkSession, because HWC
            // uses this method (why, is beyond me ...)
            if (SparkSession.getActiveSession().isEmpty()) {
                SparkSession.setActiveSession(SparkSession.builder().sparkContext(sparkContext).getOrCreate());
                clearSession = true;
            }

            if (!getOrCreateSession(sparkContext).executeUpdate(createTableStmt)) {
                throw new KNIMESparkException(String.format(
                    "Failed to create table %s (for details please check the Spark logs)", input.getHiveTableName()));
            }

            // NOTE: Always make sure that HiveWarehouseSession has been initialized before doing this
            // Currently this happens as part of building the CREATE TABLE statement, but we may not
            // be doing this forever
            dataFrame.write().format(HIVE_WAREHOUSE_CONNECTOR).mode("append")
                .option("table", input.getHiveTableName()).save();
        } finally {
            if (clearSession) {
                SparkSession.clearActiveSession();
            }
        }
    }

    private static String buildCreateTableStatement(final SparkContext sparkContext, final Dataset<Row> dataFrame,
        final Spark2HiveJobInput input) {
        final CreateTableBuilder createTableBuilder =
                getOrCreateSession(sparkContext).createTable(input.getHiveTableName()).ifNotExists();

        // we do this to inject quotes into the CREATE TABLE statement...
        for (StructField field : dataFrame.schema().fields()) {
            createTableBuilder.column(String.format("`%s`", field.name()),
                SchemaUtil.getHiveType(field.dataType(), field.metadata()));
        }

        final String createTableStmt = createTableBuilder.toString();
        return createTableStmt;
    }
}
