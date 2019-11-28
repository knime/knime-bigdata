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

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameWriter;
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
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;

/**
 * Utility class to read/write DataFrames from/to Hive using the (Hortonworks-specific) Hive Warehouse Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class HiveWarehouseSessionUtil {

    private final static Logger LOG = Logger.getLogger(HiveWarehouseSession.class);

    private static HiveWarehouseSession sessionInstance;

    private static synchronized HiveWarehouseSession getOrCreateSession(final SparkContext sparkContext) {
        if (sessionInstance == null) {
            sessionInstance =
                HiveWarehouseBuilder.session(SparkSession.builder().sparkContext(sparkContext).getOrCreate()).build();
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

        final HiveWarehouseSession session = getOrCreateSession(sparkContext);

        // we are currently building our own CREATE TABLE statement to be able to quote column
        // names and support compression formats. As HWC becomes more featureful this may not be
        // necessary anymore.
        final String createTableStmt = buildCreateTableStatement(session, dataFrame, input);

        boolean clearSession = false;
        try {
            // we need to ensure SparkSession.getActiveSession() returns a SparkSession, because HWC
            // uses this method (why, is beyond me ...)
            if (SparkSession.getActiveSession().isEmpty()) {
                SparkSession.setActiveSession(SparkSession.builder().sparkContext(sparkContext).getOrCreate());
                clearSession = true;
            }

            if (!session.executeUpdate(createTableStmt)) {
                throw new KNIMESparkException(String.format(
                    "Failed to create table %s (for details please check the Spark logs)", input.getHiveTableName()));
            }

            // NOTE: Always make sure that HiveWarehouseSession has been initialized before doing this
            // Currently this happens as part of building the CREATE TABLE statement, but we may not
            // be doing this forever
            final DataFrameWriter<Row> writer = dataFrame.write().format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR).mode("append");

            // NOTE: HWC use getTables and equalsIgnoreCase to check if the table already exists. The escaping must
            // be removed to support this check, otherwise HWC creates the table again and fails in HDP 3.1.4+ with
            // an exception that the table already exists. In versions before HDP 3.1.4, this error will be logged
            // as an error and ignored.
            writer.option("table", input.getHiveTableName().replace("`", ""));

            // Hopefully enough workarounds applied, do it.
            writer.save();

        } finally {
            if (clearSession) {
                SparkSession.clearActiveSession();
            }
        }
    }

    private static String buildCreateTableStatement(final HiveWarehouseSession session,
        final Dataset<Row> dataFrame, final Spark2HiveJobInput input) {

        final String[] dbAndTable = parseTableString(input.getHiveTableName());

        synchronized (session) {
            session.setDatabase(dbAndTable[0]);
            final CreateTableBuilder createTableBuilder = session.createTable(dbAndTable[1]).ifNotExists();

            // we do this to inject quotes into the CREATE TABLE statement...
            for (StructField field : dataFrame.schema().fields()) {
                createTableBuilder.column(String.format("`%s`", field.name()),
                    SchemaUtil.getHiveType(field.dataType(), field.metadata()));
            }
            session.setDatabase("default");
            return createTableBuilder.toString();
        }
    }

    private static String[] parseTableString(final String hiveTableName) {
        final String[] splits = hiveTableName.split("\\.");
        if (splits.length == 1) {
            return new String[]{"default", hiveTableName};
        } else if (splits.length == 2) {
            return splits;
        } else {
            return new String[]{splits[0], String.join(".", Arrays.asList(splits).subList(1, splits.length))};
        }
    }
}
