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
package org.knime.bigdata.spark1_5.jobs.database;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.database.reader.Database2SparkJobInput;
import org.knime.bigdata.spark.node.io.database.reader.Database2SparkJobOutput;
import org.knime.bigdata.spark1_5.api.NamedObjects;
import org.knime.bigdata.spark1_5.api.SparkJobWithFiles;
import org.knime.bigdata.spark1_5.api.TypeConverters;
import org.knime.bigdata.spark1_5.jobs.scripting.java.JarRegistry;

/**
 * Executes given SQL statement and puts result into a (named) RDD.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Database2SparkJob implements SparkJobWithFiles<Database2SparkJobInput, Database2SparkJobOutput> {
    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Database2SparkJob.class.getName());

    @Override
    public Database2SparkJobOutput runJob(final SparkContext sparkContext, final Database2SparkJobInput input,
            final List<File> jarFiles, final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(jarFiles);

        try {
            final String namedOutputObject = input.getFirstNamedOutputObject();
            final SQLContext sqlContext = SQLContext.getOrCreate(sparkContext);
            final DataFrame dataFrame;

            LOGGER.info("Reading jdbc table into spark rdd " + namedOutputObject);

            if (input.hasPartitioning()) {
                dataFrame = sqlContext.read().jdbc(input.getUrl(), input.getTable(),
                    input.getPartitionColumn(), input.getLowerBound(), input.getUpperBound(),
                    input.getNumPartitions(), input.getConnectionProperties());
            } else {
                dataFrame = sqlContext.read().jdbc(input.getUrl(), input.getTable(),
                    input.getConnectionProperties());
            }

            for (final StructField field : dataFrame.schema().fields()) {
                LOGGER.debug("Field '" + field.name() + "' of type '" + field.dataType() + "'");
            }

            namedObjects.addRdd(namedOutputObject, dataFrame.rdd());
            LOGGER.info("Loading JDBC table into " + namedOutputObject + " done.");

            return new Database2SparkJobOutput(namedOutputObject, TypeConverters.convertSpec(dataFrame.schema()));

        } catch(Exception e) {
            throw new KNIMESparkException("Failed to load JDBC data: " + e.getMessage(), e);
        }
    }
}
