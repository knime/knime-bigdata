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
package org.knime.bigdata.spark2_0.jobs.genericdatasource;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import org.knime.bigdata.spark2_0.api.JarRegistry;
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.SparkJobWithFiles;

/**
 * Stores the given named data frame into a path.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Spark2GenericDataSourceJob implements SparkJobWithFiles<Spark2GenericDataSourceJobInput, EmptyJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(Spark2GenericDataSourceJob.class);

    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final Spark2GenericDataSourceJobInput input,
        final List<File> inputFiles,
            final NamedObjects namedObjects) throws Exception {

        final String namedObject = input.getFirstNamedInputObject();
        final String outputPath = input.getOutputPath();

        LOGGER.info("Writing data frame " + namedObject + " into " + outputPath);

        if (!inputFiles.isEmpty()) {
            JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(inputFiles);
        }

        final Dataset<Row> dataFrame = namedObjects.getDataFrame(namedObject);
        final DataFrameWriter<Row> writer;

        if (input.overwriteNumPartitons()) {
            writer = dataFrame.coalesce(input.getNumPartitions()).write();
        } else {
            writer = dataFrame.write();
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

        LOGGER.info("Writing data frame " + namedObject + " into " + outputPath +  " done.");
        return EmptyJobOutput.getInstance();
    }
}
