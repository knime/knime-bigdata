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
 *   Created on Aug 11, 2016 by sascha
 */
package com.knime.bigdata.spark1_5.jobs.genericdatasource;

import java.io.File;
import java.util.List;

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
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import com.knime.bigdata.spark1_5.api.NamedObjects;
import com.knime.bigdata.spark1_5.api.SparkJobWithFiles;
import com.knime.bigdata.spark1_5.api.TypeConverters;
import com.knime.bigdata.spark1_5.hive.HiveContextProvider;
import com.knime.bigdata.spark1_5.hive.HiveContextProvider.HiveContextAction;
import com.knime.bigdata.spark1_5.jobs.scripting.java.JarRegistry;

/**
 * Stores the given named RDD into a path.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Spark2GenericDataSourceJob implements SparkJobWithFiles<Spark2GenericDataSourceJobInput, EmptyJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Spark2GenericDataSourceJob.class.getName());

    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final Spark2GenericDataSourceJobInput input, final List<File> inputFiles,
            final NamedObjects namedObjects) throws KNIMESparkException {

        final String namedObject = input.getFirstNamedInputObject();
        final String outputPath = getOutputPath(input);

        LOGGER.info("Writing rdd " + namedObject + " into " + outputPath);

        try {
            if (!inputFiles.isEmpty()) {
                JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(inputFiles);
            }

            final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(namedObject);
            final IntermediateSpec resultSchema = input.getSpec(namedObject);
            final StructType sparkSchema = TypeConverters.convertSpec(resultSchema);
            final DataFrame schemaPredictedData;

            if (input.useHiveContext()) {
                schemaPredictedData = HiveContextProvider.runWithHiveContext(sparkContext, new HiveContextAction<DataFrame>() {
                    @Override
                    public DataFrame runWithHiveContext(final HiveContext hiveContext) {
                        return hiveContext.createDataFrame(rowRDD, sparkSchema);
                    }
                });
            } else {
                final SQLContext sqlContext = SQLContext.getOrCreate(sparkContext);
                schemaPredictedData = sqlContext.createDataFrame(rowRDD, sparkSchema);
            }

            final DataFrameWriter writer;
            if (input.overwriteNumPartitons()) {
                writer = schemaPredictedData.coalesce(input.getNumPartitions()).write();
            } else {
                writer = schemaPredictedData.write();
            }

            writer.format(input.getFormat());
            writer.mode(SaveMode.valueOf(input.getSaveMode()));

            if (input.hasOptions()) {
                writer.options(input.getOptions());
            }

            if (input.usePartitioning()) {
                writer.partitionBy(input.getPartitionBy());
            }

            writer.save(outputPath);

        } catch (Exception e) {
            throw new KNIMESparkException(
                String.format("Failed to create output path with name '%s'. Reason: %s", outputPath, e.getMessage()));
        }

        LOGGER.info("Writing rdd " + namedObject + " into " + outputPath +  " done.");

        return new EmptyJobOutput();
    }

    private String getOutputPath(final Spark2GenericDataSourceJobInput input) {
        if (input.useDefaultFS()) {
            return FileSystem.getDefaultUri(new Configuration()).resolve(input.getOutputPath()).toString();
        } else {
            return input.getOutputPath();
        }
    }

}
