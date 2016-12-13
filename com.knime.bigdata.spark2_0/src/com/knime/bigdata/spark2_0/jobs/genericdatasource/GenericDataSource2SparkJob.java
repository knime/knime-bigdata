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
package com.knime.bigdata.spark1_6.jobs.genericdatasource;

import java.io.File;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructField;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobInput;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobOutput;
import com.knime.bigdata.spark1_6.api.NamedObjects;
import com.knime.bigdata.spark1_6.api.SparkJobWithFiles;
import com.knime.bigdata.spark1_6.api.TypeConverters;
import com.knime.bigdata.spark1_6.jobs.scripting.java.JarRegistry;

/**
 * Loads the given path into a named RDD.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class GenericDataSource2SparkJob implements SparkJobWithFiles<GenericDataSource2SparkJobInput, GenericDataSource2SparkJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(GenericDataSource2SparkJob.class.getName());

    @Override
    public GenericDataSource2SparkJobOutput runJob(final SparkContext sparkContext, final GenericDataSource2SparkJobInput input,
            final List<File> inputFiles, final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final SQLContext sqlContext = getContext(sparkContext, input);
        final String namedObject = input.getFirstNamedOutputObject();
        final String inputPath = FileSystem.getDefaultUri(new Configuration()).resolve(input.getInputPath()).toString();

        LOGGER.info("Reading path " + inputPath + " into rdd " + namedObject);

        try {
            if (!inputFiles.isEmpty()) {
                JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(inputFiles);
            }

            final DataFrameReader reader = sqlContext.read().format(input.getFormat());

            if (input.hasOptions()) {
                reader.options(input.getOptions());
            }

            final DataFrame dataFrame = reader.load(inputPath);

            for (final StructField field : dataFrame.schema().fields()) {
                LOGGER.debug("Field '" + field.name() + "' of type '" + field.dataType() + "'");
            }

            final IntermediateSpec spec = TypeConverters.convertSpec(dataFrame.schema());
            namedObjects.addJavaRdd(namedObject, dataFrame.toJavaRDD());

            LOGGER.info("Reading path " + inputPath + " into rdd " + namedObject + " done.");

            return new GenericDataSource2SparkJobOutput(namedObject, spec);

        } catch (Exception e) {
            throw new KNIMESparkException(
                String.format("Failed to read input path with name '%s'. Reason: %s", inputPath, e.getMessage()));
        }
    }

    private SQLContext getContext(final SparkContext sparkContext, final GenericDataSource2SparkJobInput jobInput) {
        if (jobInput.useHiveContext()) {
            return new HiveContext(sparkContext);
        } else {
            return SQLContext.getOrCreate(sparkContext);
        }
    }
}
