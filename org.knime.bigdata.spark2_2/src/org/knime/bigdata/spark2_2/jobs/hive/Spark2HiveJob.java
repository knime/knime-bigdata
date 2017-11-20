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
package org.knime.bigdata.spark2_2.jobs.hive;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Converts the given named data frame into a Hive table.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Spark2HiveJob implements SimpleSparkJob<Spark2HiveJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(Spark2HiveJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final Spark2HiveJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException {

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        ensureHiveSupport(spark);

        final String namedObject = input.getFirstNamedInputObject();
        final Dataset<Row> dataset = namedObjects.getDataFrame(namedObject);
        final String hiveTableName = input.getHiveTableName();
        LOGGER.info("Writing hive table: " + hiveTableName);

        try {
            // DataFrame.saveAsTable() creates a table in the Hive Metastore, which is /only/ readable by Spark, but not Hive
            // itself, due to being parquet-encoded in a way that is incompatible with Hive. This issue has been mentioned on the
            // Spark mailing list:
            // http://mail-archives.us.apache.org/mod_mbox/spark-user/201504.mbox/%3cCANpNmWVDpbY_UQQTfYVieDw8yp9q4s_PoOyFzqqSnL__zDO_Rw@mail.gmail.com%3e
            // The solution is to manually create a Hive table with an SQL statement:
            String tmpTable = "tmpTable" + UUID.randomUUID().toString().replaceAll("-", "");
            dataset.createTempView(tmpTable);
            spark.sql(String.format("CREATE TABLE %s AS SELECT * FROM %s", hiveTableName, tmpTable));

        } catch (Exception e) {
            throw new KNIMESparkException(
                String.format("Failed to create hive table with name '%s'. Reason: %s", hiveTableName, e.getMessage()));
        }

        LOGGER.info("Hive table: " + hiveTableName +  " created");
    }

    private void ensureHiveSupport(final SparkSession spark) throws KNIMESparkException {
        if (!spark.conf().get("spark.sql.catalogImplementation", "in-memory").equals("hive")) {
            throw new KNIMESparkException("Spark session does not support hive!"
                + " Please set spark.sql.catalogImplementation = \"hive\" in environment.conf.");
        }
    }
}
