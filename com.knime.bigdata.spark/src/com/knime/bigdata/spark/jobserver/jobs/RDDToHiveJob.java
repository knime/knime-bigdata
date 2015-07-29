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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

/**
 * Converts the given named RDD into a Hive table.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class RDDToHiveJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_DATA_FILE_NAME = ParameterConstants.PARAM_INPUT + "."
            + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_DATA_SCHEMA = ParameterConstants.PARAM_INPUT + "."
            + ParameterConstants.PARAM_SCHEMA;

    private static final String PARAM_RESULT_TABLE_NAME = ParameterConstants.PARAM_OUTPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(RDDToHiveJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final Config config) {
        String msg = null;
        if (!config.hasPath(PARAM_DATA_FILE_NAME)) {
            msg = "Input parameter '" + PARAM_DATA_FILE_NAME + "' missing.";
        }
        if (msg == null && !config.hasPath(PARAM_DATA_SCHEMA)) {
            msg = "Input parameter '" + PARAM_DATA_SCHEMA + "' missing.";
        }
        if (msg == null && !config.hasPath(PARAM_RESULT_TABLE_NAME)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE_NAME + "' missing.";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * run the actual job, the result is serialized back to the client the true result is a Hive data table that
     * contains the data of the incoming rdd
     *
     * @return the JobResult
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        LOGGER.log(Level.INFO, "writing hive table...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getString(PARAM_DATA_FILE_NAME));
        String schemaString = aConfig.getString(PARAM_DATA_SCHEMA);
        Config schemaConfig = ConfigFactory.parseString(schemaString);
        ConfigList types = schemaConfig.getList(ParameterConstants.PARAM_SCHEMA);
        StructField[] fields = new StructField[types.size()];
        int f = 0;
        for (ConfigValue v : types) {
            List<Object> dt = ((ConfigList)v).unwrapped();
            DataType t;
            try {
                t = StructTypeBuilder.DATA_TYPES_BY_CLASS.get(Class.forName(dt.get(1).toString()));
                fields[f++] =
                    DataType.createStructField(dt.get(0).toString(), t, Boolean.parseBoolean(dt.get(2).toString()));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        final String hiveTableName = aConfig.getString(PARAM_RESULT_TABLE_NAME);
        try {
            StructType resultSchema = DataType.createStructType(fields);
            final JavaHiveContext hiveContext = new JavaHiveContext(JavaSparkContext.fromSparkContext(sc));
            final JavaSchemaRDD schemaPredictedData = hiveContext.applySchema(rowRDD, resultSchema);
                schemaPredictedData.saveAsTable(hiveTableName);
        } catch (Exception e) {
            String msg = "Failed to create hive table with name '" + hiveTableName + "'. Exception: ";
            if (e instanceof AlreadyExistsException) {
                throw new GenericKnimeSparkException(msg + "Table already exists");
            }
            throw new GenericKnimeSparkException(msg + e.getMessage());
        }
        return JobResult.emptyJobResult().withMessage("OK");
    }
}
