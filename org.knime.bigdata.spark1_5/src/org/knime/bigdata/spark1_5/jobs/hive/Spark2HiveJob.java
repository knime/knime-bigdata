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
package org.knime.bigdata.spark1_5.jobs.hive;

import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.bigdata.spark.node.io.hive.writer.ParquetCompression;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;
import org.knime.bigdata.spark1_5.api.NamedObjects;
import org.knime.bigdata.spark1_5.api.SimpleSparkJob;
import org.knime.bigdata.spark1_5.api.TypeConverters;
import org.knime.bigdata.spark1_5.hive.HiveContextProvider;
import org.knime.bigdata.spark1_5.hive.HiveContextProvider.HiveContextAction;

/**
 * Converts the given named RDD into a Hive table.
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
        throws KNIMESparkException {

        final String namedObject = input.getFirstNamedInputObject();
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(namedObject);
        final IntermediateSpec resultSchema = input.getSpec(namedObject);
        final StructType sparkSchema = TypeConverters.convertSpec(resultSchema);
        final String hiveTableName = input.getHiveTableName();
        final String fileFormat = input.getFileFormat();
        final String compression = input.getCompression();

        LOGGER.log(Level.INFO, "Writing hive table: {0}", hiveTableName);
        HiveContextProvider.runWithHiveContext(sparkContext, new HiveContextAction<Void>() {
            @Override
            public Void runWithHiveContext(final HiveContext hiveContext) {

                final DataFrame schemaPredictedData = hiveContext.createDataFrame(rowRDD, sparkSchema);

                // DataFrame.saveAsTable() creates a table in the Hive Metastore, which is /only/ readable by Spark,
                // but not Hive itself, due to being parquet-encoded in a way that is incompatible with Hive.
                // This issue has been mentioned on the Spark mailing list:
                // http://mail-archives.us.apache.org/mod_mbox/spark-user/201504.mbox/%3cCANpNmWVDpbY_UQQTfYVieDw8yp9q4s_PoOyFzqqSnL__zDO_Rw@mail.gmail.com%3e
                // The solution is to manually create a Hive table with an SQL statement:
                String tmpTable = "tmpTable" + UUID.randomUUID().toString().replaceAll("-", "");
                schemaPredictedData.registerTempTable(tmpTable);

                String sqlStatement = buildCreateTableSatement(hiveTableName, fileFormat, compression, tmpTable);

                if (FileFormat.valueOf(fileFormat) == FileFormat.PARQUET) {
                    runWithParquetcompression(hiveContext, sqlStatement, compression);
                } else {
                    hiveContext.sql(sqlStatement);
                }
                hiveContext.dropTempTable(tmpTable);
                return null;
            }

        });

        LOGGER.log(Level.INFO, "Hive table: {0} created", hiveTableName);
    }

    /**
     * Setting the Parquet compression global, so we lock the access and reset compression afterwards.
     */
    private void runWithParquetcompression(final HiveContext hiveContext, final String sqlStatement,
        final String compression) {
        String previousCompression = "";
        LOCK.lock();
        try {
            // Remember previous compression setting
            previousCompression = hiveContext.getConf(SPARK_SQL_PARQUET_COMPRESSION_CODEC, null);

            // Set compression
            ParquetCompression parquetCompression = ParquetCompression.valueOf(compression);
            String codecString = parquetCompression.getParquetCompressionCodec();
            hiveContext.setConf(SPARK_SQL_PARQUET_COMPRESSION_CODEC, codecString);
            LOGGER.log(Level.INFO, "Setting PARQUET compression {0}.", codecString);

            // Now run SQL statement
            hiveContext.sql(sqlStatement);
        } finally {

            // Reset compression and release lock
            resetParquetCompression(hiveContext, previousCompression);
            LOCK.unlock();
        }
    }

    private void resetParquetCompression(final HiveContext hiveContext, final String previousCompression) {
        try {
            if (previousCompression == null) {
                // There is no unset() in Spark 1.*.
                // Set it back to the default given in the spark documentation
                hiveContext.setConf(SPARK_SQL_PARQUET_COMPRESSION_CODEC, ParquetCompression.GZIP.toString());
            } else if (!previousCompression.isEmpty()) {
                hiveContext.setConf(SPARK_SQL_PARQUET_COMPRESSION_CODEC, previousCompression);
                LOGGER.log(Level.INFO, "Setting PARQUET compression back to {0}.", previousCompression);
            }
        } catch (Exception e) {
            // Do nothing
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
            sqlStatement.append(String.format(" TBLPROPERTIES (\"orc.compress\"=\"%s\" )", compression));
        }
        sqlStatement.append(String.format(" AS SELECT * FROM %s", tmpTable));

        return sqlStatement.toString();
    }
}
