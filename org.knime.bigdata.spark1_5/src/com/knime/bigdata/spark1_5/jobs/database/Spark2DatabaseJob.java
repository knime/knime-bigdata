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
package com.knime.bigdata.spark1_5.jobs.database;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.io.database.writer.Spark2DatabaseJobInput;
import com.knime.bigdata.spark1_5.api.NamedObjects;
import com.knime.bigdata.spark1_5.api.SparkJobWithFiles;
import com.knime.bigdata.spark1_5.api.TypeConverters;
import com.knime.bigdata.spark1_5.jobs.scripting.java.JarRegistry;

/**
 * Write given (named) RDD into JDBC table.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Spark2DatabaseJob implements SparkJobWithFiles<Spark2DatabaseJobInput, EmptyJobOutput> {
    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Spark2DatabaseJob.class.getName());

    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final Spark2DatabaseJobInput input,
            final List<File> jarFiles, final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(jarFiles);

        try {
            final String namedInputObject = input.getFirstNamedInputObject();
            final SQLContext sqlContext = SQLContext.getOrCreate(sparkContext);
            final RDD<Row> rowRdd = namedObjects.getRdd(namedInputObject);
            final IntermediateSpec resultSchema = input.getSpec(namedInputObject);
            final StructType sparkSchema = TypeConverters.convertSpec(resultSchema);

            LOGGER.info("Writing spark rdd " + namedInputObject + " into " + input.getTable());

            sqlContext.createDataFrame(rowRdd, sparkSchema)
                .write()
                .mode(SaveMode.valueOf(input.getSaveMode()))
                .jdbc(input.getUrl(), input.getTable(), input.getConnectionProperties());

            LOGGER.info("Writing " + namedInputObject + " into JDBC table done.");

        } catch(Exception e) {
            throw new KNIMESparkException("Failed to load JDBC data: " + e.getMessage(), e);
        }

        return EmptyJobOutput.getInstance();
    }
}
