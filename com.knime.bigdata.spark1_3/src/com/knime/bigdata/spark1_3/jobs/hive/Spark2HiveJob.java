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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark1_3.jobs.hive;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;
import com.knime.bigdata.spark1_3.base.NamedObjects;
import com.knime.bigdata.spark1_3.base.SimpleSparkJob;
import com.knime.bigdata.spark1_3.converter.type.TypeConverters;

/**
 * Converts the given named RDD into a Hive table.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Spark2HiveJob implements SimpleSparkJob<Spark2HiveJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Spark2HiveJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final Spark2HiveJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        final String namedObject = input.getFirstNamedInputObject();
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(namedObject);
        final IntermediateSpec resultSchema = input.getSpec(namedObject);
        StructType sparkSchema = TypeConverters.convertSpec(resultSchema);
        final String hiveTableName = input.getHiveTableName();


        LOGGER.log(Level.INFO, "Writing hive table: " + hiveTableName);
        try {
            final HiveContext hiveContext = new HiveContext(sparkContext);
            final DataFrame schemaPredictedData = hiveContext.createDataFrame(rowRDD, sparkSchema);

            // DataFrame.saveAsTable() creates a table in the Hive Metastore, which is /only/ readable by Spark, but not Hive
            // itself, due to being parquet-encoded in a way that is incompatible with Hive. This issue has been mentioned on the
            // Spark mailing list:
            // http://mail-archives.us.apache.org/mod_mbox/spark-user/201504.mbox/%3cCANpNmWVDpbY_UQQTfYVieDw8yp9q4s_PoOyFzqqSnL__zDO_Rw@mail.gmail.com%3e
            // The solution is to manually create a Hive table with an SQL statement:
            String tmpTable = "tmpTable" + UUID.randomUUID().toString().replaceAll("-", "");
            schemaPredictedData.registerTempTable(tmpTable);
            hiveContext.sql(String.format("CREATE TABLE %s AS SELECT * FROM %s", hiveTableName, tmpTable));
            hiveContext.dropTempTable(tmpTable);
        } catch (Exception e) {
            throw new KNIMESparkException(
                String.format("Failed to create hive table with name '%s'. Reason: %s", hiveTableName, e.getMessage()));
        }
        LOGGER.log(Level.INFO, "Hive table: " + hiveTableName +  " created");
    }
}
