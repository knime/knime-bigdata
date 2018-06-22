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
package org.knime.bigdata.spark2_3.jobs.sql;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.sql.SparkSQLJobInput;
import org.knime.bigdata.spark.node.sql.SparkSQLJobOutput;
import org.knime.bigdata.spark2_3.api.NamedObjects;
import org.knime.bigdata.spark2_3.api.SparkJob;
import org.knime.bigdata.spark2_3.api.TypeConverters;

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
            final SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

            final String tempTable = "sparkSQLJob_" + UUID.randomUUID().toString().replace('-', '_');
            final String query = input.getQuery(tempTable);

            LOGGER.info("Running Spark SQL query: " + query);

            final String namedInputObject = input.getFirstNamedInputObject();
            final Dataset<Row> inputDataFrame = namedObjects.getDataFrame(namedInputObject);

            inputDataFrame.createTempView(tempTable);
            try {
                final Dataset<Row> outputDataFrame = sparkSession.sql(query);
                final String namedOutputObject = input.getFirstNamedOutputObject();
                final IntermediateSpec outputSchema = TypeConverters.convertSpec(outputDataFrame.schema());
                namedObjects.addDataFrame(namedOutputObject, outputDataFrame);
                LOGGER.info("Running Spark SQL query done with data frame: " + namedOutputObject);
                return new SparkSQLJobOutput(namedOutputObject, outputSchema);
            } finally {
                sparkSession.catalog().dropTempView(tempTable);
            }
        } catch(Exception e) {
            throw new KNIMESparkException("Failed to execute Spark SQL query: " + e.getMessage(), e);
        }
    }
}
