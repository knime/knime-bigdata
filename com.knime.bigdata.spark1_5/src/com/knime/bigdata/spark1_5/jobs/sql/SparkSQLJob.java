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
 */
package com.knime.bigdata.spark1_5.jobs.sql;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.sql.SparkSQLJobInput;
import com.knime.bigdata.spark.node.sql.SparkSQLJobOutput;
import com.knime.bigdata.spark1_5.api.NamedObjects;
import com.knime.bigdata.spark1_5.api.SparkJob;
import com.knime.bigdata.spark1_5.api.TypeConverters;

/**
 * Executes a Spark SQL query.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class SparkSQLJob implements SparkJob<SparkSQLJobInput, SparkSQLJobOutput> {
    private final static long serialVersionUID = 1L;
    private final static Logger LOGGER = Logger.getLogger(SparkSQLJob.class.getName());

    @Override
    public SparkSQLJobOutput runJob(final SparkContext sparkContext, final SparkSQLJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException, Exception {

        try {
            final SQLContext sqlContext = SQLContext.getOrCreate(sparkContext);

            final String tempTable = "sparkSQLJob_" + UUID.randomUUID().toString().replace('-', '_');
            final String query = input.getQuery(tempTable);

            LOGGER.info("Running Spark SQL query: " + query);

            final String namedInputObject = input.getFirstNamedInputObject();
            final RDD<Row> rowRdd = namedObjects.getRdd(namedInputObject);
            final IntermediateSpec inputSchema = input.getSpec(namedInputObject);
            final StructType sparkSchema = TypeConverters.convertSpec(inputSchema);
            final DataFrame inputDataFrame = sqlContext.createDataFrame(rowRdd, sparkSchema);

            inputDataFrame.registerTempTable(tempTable);
            try {
                final DataFrame outputDataFrame = sqlContext.sql(query);
                final String namedOutputObject = input.getFirstNamedOutputObject();
                final IntermediateSpec outputSchema = TypeConverters.convertSpec(outputDataFrame.schema());
                namedObjects.addJavaRdd(namedOutputObject, outputDataFrame.toJavaRDD());
                LOGGER.info("Running Spark SQL query done with RDD: " + namedOutputObject);
                return new SparkSQLJobOutput(namedOutputObject, outputSchema);
            }finally {
                sqlContext.dropTempTable(tempTable);
            }
        } catch(Exception e) {
            throw new KNIMESparkException("Failed to execute Spark SQL query: " + e.getMessage(), e);
        }
    }
}
