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
 */
package org.knime.bigdata.spark1_3.jobs.sql;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.sql.SparkSQLJobInput;
import org.knime.bigdata.spark.node.sql.SparkSQLJobOutput;
import org.knime.bigdata.spark1_3.api.NamedObjects;
import org.knime.bigdata.spark1_3.api.SparkJob;
import org.knime.bigdata.spark1_3.api.TypeConverters;
import org.knime.bigdata.spark1_3.hive.HiveContextProvider;
import org.knime.bigdata.spark1_3.hive.HiveContextProvider.HiveContextAction;

/**
 * Executes a Spark SQL query.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class SparkSQLJob implements SparkJob<SparkSQLJobInput, SparkSQLJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(SparkSQLJob.class.getName());

    @Override
    public SparkSQLJobOutput runJob(final SparkContext sparkContext, final SparkSQLJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException {

        try {
            final String tempTable = "sparkSQLJob_" + UUID.randomUUID().toString().replace('-', '_');
            final String query = input.getQuery(tempTable);

            LOGGER.info("Running Spark SQL query: " + query);

            final String namedInputObject = input.getFirstNamedInputObject();
            final RDD<Row> rowRdd = namedObjects.getRdd(namedInputObject);
            final IntermediateSpec inputSchema = input.getSpec(namedInputObject);
            final StructType sparkSchema = TypeConverters.convertSpec(inputSchema);

            final DataFrame outputDataFrame = HiveContextProvider.runWithHiveContext(sparkContext, new HiveContextAction<DataFrame>() {
                @Override
                public DataFrame runWithHiveContext(final HiveContext hiveContext) {
                    try {
                        final DataFrame inputDataFrame = hiveContext.createDataFrame(rowRdd, sparkSchema);

                        // BD-778: dropTempView (see below) might change the persistence of the input data frame,
                        // that's why we have to use a dummy here
                        final DataFrame dummyDataFrame = inputDataFrame.select("*");

                        dummyDataFrame.registerTempTable(tempTable);
                        return hiveContext.sql(query);
                    } finally {
                        hiveContext.dropTempTable(tempTable);
                    }
                }
            });

            final String namedOutputObject = input.getFirstNamedOutputObject();
            final IntermediateSpec outputSchema = TypeConverters.convertSpec(outputDataFrame.schema());
            namedObjects.addJavaRdd(namedOutputObject, outputDataFrame.toJavaRDD());
            LOGGER.info("Running Spark SQL query done with RDD: " + namedOutputObject);
            return new SparkSQLJobOutput(namedOutputObject, outputSchema);
        } catch(Exception e) {
            throw new KNIMESparkException("Failed to execute Spark SQL query: " + e.getMessage(), e);
        }
    }
}
