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
package com.knime.bigdata.spark1_2.jobs.hive;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;
import com.knime.bigdata.spark1_2.api.NamedObjects;
import com.knime.bigdata.spark1_2.api.SimpleSparkJob;
import com.knime.bigdata.spark1_2.api.TypeConverters;

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
    	final JavaHiveContext hiveContext = new JavaHiveContext(JavaSparkContext.fromSparkContext(sparkContext));
        final JavaSchemaRDD schemaPredictedData = hiveContext.applySchema(rowRDD, sparkSchema);

        // JavaSchemaRDD.saveAsTable() creates a table in the Hive Metastore, which may not be Hive
        // readable. The solution is to register a temp table and manually create a Hive table with an SQL statement:
        String tmpTable = "tmpTable" + UUID.randomUUID().toString().replaceAll("-", "");
        schemaPredictedData.registerTempTable(tmpTable);
        hiveContext.sql(String.format("CREATE TABLE %s AS SELECT * FROM %s", hiveTableName, tmpTable));
        hiveContext.sqlContext().dropTempTable(tmpTable);
        LOGGER.log(Level.INFO, "Hive table: " + hiveTableName +  " created");
    }
}
