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
package com.knime.bigdata.spark1_6.jobs.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.io.parquet.writer.Spark2ParquetJobInput;
import com.knime.bigdata.spark1_6.api.NamedObjects;
import com.knime.bigdata.spark1_6.api.SimpleSparkJob;
import com.knime.bigdata.spark1_6.api.TypeConverters;

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
        final String parquetPath = getOutputPath(input);

        LOGGER.info("Writing parquet table into " + parquetPath);
        try {
            final SQLContext sqlContext = new SQLContext(sparkContext);
            final DataFrame schemaPredictedData = sqlContext.createDataFrame(rowRDD, sparkSchema);
            final DataFrameWriter writer = schemaPredictedData.write();
            writer.mode(SaveMode.valueOf(input.getSaveMode()));
            writer.parquet(parquetPath);

        } catch (Exception e) {
            throw new KNIMESparkException(
                String.format("Failed to create parquet table with name '%s'. Reason: %s", parquetPath, e.getMessage()));
        }

        LOGGER.info("Parquet table in " + parquetPath +  " created");
    }

    private String getOutputPath(final Spark2ParquetJobInput input) throws KNIMESparkException {
        try {
            if (input.isHDFSPath()) {
                return FileSystem.getDefaultUri(new Configuration()).resolve(input.getOutputPath()).toString();
            } else {
                return "file://" + input.getOutputPath();
            }
        } catch (IllegalArgumentException e) {
            throw new KNIMESparkException(
                String.format("Unable to construct output path '%s'. Reason: %s", input.getOutputPath(), e.getMessage()));
        }
    }
}
