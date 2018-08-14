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
package org.knime.bigdata.spark1_5.jobs.genericdatasource;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobInput;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobOutput;
import org.knime.bigdata.spark1_5.api.NamedObjects;
import org.knime.bigdata.spark1_5.api.SparkJobWithFiles;
import org.knime.bigdata.spark1_5.api.TypeConverters;
import org.knime.bigdata.spark1_5.hive.HiveContextProvider;
import org.knime.bigdata.spark1_5.hive.HiveContextProvider.HiveContextAction;
import org.knime.bigdata.spark1_5.jobs.scripting.java.JarRegistry;

/**
 * Loads the given path into a named RDD.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class GenericDataSource2SparkJob implements SparkJobWithFiles<GenericDataSource2SparkJobInput, GenericDataSource2SparkJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(GenericDataSource2SparkJob.class);

    @Override
    public GenericDataSource2SparkJobOutput runJob(final SparkContext sparkContext, final GenericDataSource2SparkJobInput input,
            final List<File> inputFiles, final NamedObjects namedObjects) throws Exception {

        final String namedObject = input.getFirstNamedOutputObject();
        final String inputPath = input.getInputPath();
        final DataFrame dataFrame;

        LOGGER.info("Reading path " + inputPath + " into rdd " + namedObject);

        try {
            if (!inputFiles.isEmpty()) {
                JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(inputFiles);
            }

            if (input.useHiveContext()) {
                dataFrame = HiveContextProvider.runWithHiveContext(sparkContext, new HiveContextAction<DataFrame>() {
                    @Override
                    public DataFrame runWithHiveContext(final HiveContext hiveContext) {
                        final DataFrameReader reader = hiveContext.read().format(input.getFormat());

                        if (input.hasOptions()) {
                            reader.options(input.getOptions());
                        }

                        return reader.load(inputPath);
                    }
                });
            } else {
                final SQLContext sqlContext = SQLContext.getOrCreate(sparkContext);
                final DataFrameReader reader = sqlContext.read().format(input.getFormat());

                if (input.hasOptions()) {
                    reader.options(input.getOptions());
                }

                dataFrame = reader.load(inputPath);
            }

            for (final StructField field : dataFrame.schema().fields()) {
                LOGGER.debug("Field '" + field.name() + "' of type '" + field.dataType() + "'");
            }

            final IntermediateSpec spec = TypeConverters.convertSpec(dataFrame.schema());
            namedObjects.addJavaRdd(namedObject, dataFrame.toJavaRDD());

            LOGGER.info("Reading path " + inputPath + " into rdd " + namedObject + " done.");

            return new GenericDataSource2SparkJobOutput(namedObject, spec);

        } catch (Exception e) {
            if (e instanceof AnalysisException) {
                // special characters are known to cause an AnalysisException with Spark to ORC and ORC to Spark nodes (see BD-701)
                throw new KNIMESparkException(String.format(
                    "Failed to read input path with name '%s'. This might be caused by special characters in column names.",
                    inputPath, e.getMessage()), e);
            } else {
                throw e;
            }
        }
    }
}
