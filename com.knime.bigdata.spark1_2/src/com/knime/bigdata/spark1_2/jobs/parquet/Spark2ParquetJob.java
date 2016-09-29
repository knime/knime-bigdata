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
 *   Created on Aug 11, 2016 by sascha
 */
package com.knime.bigdata.spark1_2.jobs.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.io.parquet.writer.Spark2ParquetJobInput;
import com.knime.bigdata.spark1_2.api.NamedObjects;
import com.knime.bigdata.spark1_2.api.SimpleSparkJob;
import com.knime.bigdata.spark1_2.api.TypeConverters;

/**
 * Converts the given named RDD into a parquet table.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Spark2ParquetJob implements SimpleSparkJob<Spark2ParquetJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Spark2ParquetJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final Spark2ParquetJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        final String namedObject = input.getFirstNamedInputObject();
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(namedObject);
        final IntermediateSpec resultSchema = input.getSpec(namedObject);
        StructType sparkSchema = TypeConverters.convertSpec(resultSchema);
        final String parquetPath = FileSystem.getDefaultUri(new Configuration()).resolve(input.getOutputPath()).toString();

        LOGGER.info("Writing parquet table into " + parquetPath);

        if (!input.getSaveMode().equalsIgnoreCase("ErrorIfExists")) {
            LOGGER.warn("Spark 1.2 does not support save mode on parquet files. Save mode settings ignored.");
        }

        try {
            final JavaSQLContext sqlContext = new JavaSQLContext(JavaSparkContext.fromSparkContext(sparkContext));
            final JavaSchemaRDD schemaPredictedData = sqlContext.applySchema(rowRDD, sparkSchema);
            schemaPredictedData.saveAsParquetFile(parquetPath);

        } catch (Exception e) {
            throw new KNIMESparkException(
                String.format("Failed to create parquet table with name '%s'. Reason: %s", parquetPath, e.getMessage()));
        }

        LOGGER.info("Parquet table in " + parquetPath +  " created");
    }
}
