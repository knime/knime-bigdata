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
package org.knime.bigdata.spark3_0.jobs.hive;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.bigdata.spark.node.io.hive.writer.ParquetCompression;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;
import org.knime.bigdata.spark3_0.api.NamedObjects;
import org.knime.bigdata.spark3_0.api.SimpleSparkJob;

/**
 * Converts the given named data frame into a Hive table.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Spark2HiveJob implements SimpleSparkJob<Spark2HiveJobInput> {

    private static final String SPARK_SQL_PARQUET_COMPRESSION_CODEC = "spark.sql.parquet.compression.codec";

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(Spark2HiveJob.class.getName());

    private static final ReentrantLock LOCK = new ReentrantLock();

    @Override
    public void runJob(final SparkContext sparkContext, final Spark2HiveJobInput input, final NamedObjects namedObjects)
        throws Exception {

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

        final String namedObject = input.getFirstNamedInputObject();
        final Dataset<Row> dataset = namedObjects.getDataFrame(namedObject);
        final String hiveTableName = input.getHiveTableName();
        final String fileFormat = input.getFileFormat();
        final String compression = input.getCompression();

        LOGGER.info(
            String.format("Writing hive table using Hive session: %s stored as %s", hiveTableName, fileFormat));
        writeUsingHiveSession(spark, dataset, hiveTableName, fileFormat, compression);

        LOGGER.info("Hive table: " + hiveTableName + " created");
    }

    private void writeUsingHiveSession(final SparkSession spark, final Dataset<Row> dataset, final String hiveTableName,
        final String fileFormat, final String compression) throws Exception {

        ensureHiveSupport(spark);

        // DataFrame.saveAsTable() creates a table in the Hive Metastore, which is /only/ readable by Spark,
        // but not Hive itself, due to being parquet-encoded in a way that is incompatible with Hive.
        // This issue has been mentioned on the Spark mailing list:
        // http://mail-archives.us.apache.org/mod_mbox/spark-user/201504.mbox/%3cCANpNmWVDpbY_UQQTfYVieDw8yp9q4s_PoOyFzqqSnL__zDO_Rw@mail.gmail.com%3e
        // The solution is to manually create a Hive table with an SQL statement:
        String tmpTable = "tmpTable" + UUID.randomUUID().toString().replaceAll("-", "");
        dataset.createTempView(tmpTable);
        String sqlStatement = buildCreateTableSatement(hiveTableName, fileFormat, compression, tmpTable);

        if (FileFormat.valueOf(fileFormat) == FileFormat.PARQUET) {
            runWithParquetcompression(spark, sqlStatement, compression);
        } else {
            spark.sql(sqlStatement);
        }
    }

    /**
     * Setting the Parquet compression global, so we lock the access and reset compression afterwards.
     */
    private void runWithParquetcompression(final SparkSession spark, final String sqlStatement,
        final String compression) {
        LOCK.lock();
        String previousCompression = "";
        try {
            // Remember previous compression setting
            try {
                previousCompression = spark.conf().get(SPARK_SQL_PARQUET_COMPRESSION_CODEC);
                LOGGER.debug(String.format("Found PARQUET compression %s.", previousCompression));
            } catch (NoSuchElementException e) {
                LOGGER.debug("No previous PARQUET compression found.");
                //Nothing to do
            }
            // Set compression
            ParquetCompression parquetCompression = ParquetCompression.valueOf(compression);
            String codecString = parquetCompression.getParquetCompressionCodec();
            spark.conf().set(SPARK_SQL_PARQUET_COMPRESSION_CODEC, codecString);
            LOGGER.info("Setting PARQUET compression " + codecString);

            // Now run SQL statement
            spark.sql(sqlStatement);
        } finally {

            // Reset compression and release lock
            resetParquetCompression(spark, previousCompression);
            LOCK.unlock();
        }
    }

    private void resetParquetCompression(final SparkSession spark, final String previousCompression) {
        try {
            if (previousCompression.isEmpty()) {
                LOGGER.info("Unsetting PARQUET compression");
                spark.conf().unset(SPARK_SQL_PARQUET_COMPRESSION_CODEC);
            } else {
                spark.conf().set(SPARK_SQL_PARQUET_COMPRESSION_CODEC, previousCompression);
                LOGGER.info(String.format("Setting PARQUET compression back to %s.", previousCompression));
            }
        } catch (Exception e) {
            // Do nothing.
        }
    }

    private String buildCreateTableSatement(final String hiveTableName, final String fileFormat,
        final String compression, final String tmpTable) {

        final FileFormat format = FileFormat.valueOf(fileFormat);

        final StringBuilder sqlStatement = new StringBuilder(String.format("CREATE TABLE %s", hiveTableName));
        if (format != FileFormat.CLUSTER_DEFAULT) {
            sqlStatement.append(String.format(" STORED AS %s", fileFormat));
        }
        if (format == FileFormat.ORC) {
            sqlStatement.append(
                String.format(" TBLPROPERTIES (\"transactional\"=\"false\", \"orc.compress\"=\"%s\" )", compression));
        } else {
            sqlStatement.append(" TBLPROPERTIES (\"transactional\"=\"false\")");
        }
        sqlStatement.append(String.format(" AS SELECT * FROM %s", tmpTable));

        return sqlStatement.toString();
    }

    private void ensureHiveSupport(final SparkSession spark) throws KNIMESparkException {
        if (!spark.conf().get("spark.sql.catalogImplementation", "in-memory").equals("hive")) {
            throw new KNIMESparkException("Spark session does not support hive!"
                + " Please set spark.sql.catalogImplementation = \"hive\".");
        }
    }
}
