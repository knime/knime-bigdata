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
package com.knime.bigdata.spark1_3.jobs.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.io.parquet.reader.Parquet2SparkJobInput;
import com.knime.bigdata.spark.node.io.parquet.reader.Parquet2SparkJobOutput;
import com.knime.bigdata.spark1_3.api.NamedObjects;
import com.knime.bigdata.spark1_3.api.SparkJob;
import com.knime.bigdata.spark1_3.api.TypeConverters;

/**
 * Converts the given parquet table into a named RDD.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Parquet2SparkJob implements SparkJob<Parquet2SparkJobInput, Parquet2SparkJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Parquet2SparkJob.class.getName());

    @Override
    public Parquet2SparkJobOutput runJob(final SparkContext sparkContext, final Parquet2SparkJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final SQLContext sqlContext = new SQLContext(sparkContext);
        final String parquetFile = FileSystem.getDefaultUri(new Configuration()).resolve(input.getInputPath()).toString();

        LOGGER.info("Reading parquet file: " + parquetFile);
        final DataFrame dataFrame = sqlContext.parquetFile(parquetFile);

        for (final StructField field : dataFrame.schema().fields()) {
            LOGGER.debug("Field '" + field.name() + "' of type '" + field.dataType() + "'");
        }

        final IntermediateSpec spec = TypeConverters.convertSpec(dataFrame.schema());
        final String key = input.getFirstNamedOutputObject();
        LOGGER.info("Storing parquet file under key: " + key);
        namedObjects.addJavaRdd(key, dataFrame.toJavaRDD());
        LOGGER.info("done");

        return new Parquet2SparkJobOutput(input.getFirstNamedOutputObject(), spec);
    }
}
