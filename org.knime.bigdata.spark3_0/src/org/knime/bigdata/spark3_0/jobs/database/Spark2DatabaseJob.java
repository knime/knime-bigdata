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
 *   Created on Sep 06, 2016 by sascha
 */
package org.knime.bigdata.spark3_0.jobs.database;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.database.writer.Spark2DatabaseJobInput;
import org.knime.bigdata.spark3_0.api.JarRegistry;
import org.knime.bigdata.spark3_0.api.NamedObjects;
import org.knime.bigdata.spark3_0.api.SparkJobWithFiles;

/**
 * Write given (named) data frame into a JDBC table.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Spark2DatabaseJob implements SparkJobWithFiles<Spark2DatabaseJobInput, EmptyJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(Spark2DatabaseJob.class.getName());

    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final Spark2DatabaseJobInput input,
            final List<File> jarFiles, final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(jarFiles);

        try {
            final String namedInputObject = input.getFirstNamedInputObject();
            final Dataset<Row> dataFrame = namedObjects.getDataFrame(namedInputObject);

            LOGGER.info("Writing data frame " + namedInputObject + " into JDBC tabel " + input.getTable());

            dataFrame.write()
                .mode(SaveMode.valueOf(input.getSaveMode()))
                .jdbc(input.getUrl(), input.getTable(), input.getConnectionProperties());

            LOGGER.info("Writing data frame " + namedInputObject + " into JDBC table done.");
            return new EmptyJobOutput();

        } catch(Exception e) {
            throw new KNIMESparkException("Failed to load JDBC data: " + e.getMessage(), e);
        }
    }
}
