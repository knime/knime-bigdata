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
 *   Created on 24.07.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;

/**
 *
 * @author dwk
 */
public class ImportKNIMETableJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String PARAM_DATA_ARRAY = ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_1;

    static final String PARAM_JAVA_TYPES = ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_SCHEMA;

    static final String PARAM_TABLE_KEY = ParameterConstants.PARAM_OUTPUT + "." + ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(ImportKNIMETableJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;

        if (!aConfig.hasPath(PARAM_DATA_ARRAY)) {
            msg = "Input parameter '" + PARAM_DATA_ARRAY + "' missing.";
        } else {
            try {
                ConfigList data = aConfig.getList(PARAM_DATA_ARRAY);
                for (ConfigValue row : data) {
                    @SuppressWarnings("unused")
                    ConfigList s = (ConfigList)row;
                }
            } catch (ConfigException e) {
                msg =
                    "Input parameter '" + PARAM_DATA_ARRAY + "' is not of expected type 'Object[][]': "
                        + e.getMessage();
            }
        }

        if (msg == null) {
            if (!aConfig.hasPath(PARAM_JAVA_TYPES)) {
                msg = "Input parameter '" + PARAM_JAVA_TYPES + "' missing.";
            } else {
                try {
                    ConfigList types = aConfig.getList(PARAM_JAVA_TYPES);
                    for (ConfigValue type : types) {
                        final Object o = type.unwrapped();
                        if (o.equals("class java.lang.Integer") || o.equals("class java.lang.Double")
                            || o.equals("class java.lang.Boolean") || o.equals("class java.lang.String")) {

                        } else {
                            msg = "Input parameter '" + PARAM_JAVA_TYPES + "' has unexpected entry: " + type.toString();
                        }
                    }
                } catch (ConfigException e) {
                    msg = "Input parameter '" + PARAM_JAVA_TYPES + "' is not of expected type 'Object[]'.";
                }
            }
        }

        if (msg == null && !aConfig.hasPath(PARAM_TABLE_KEY)) {
            msg = "Output parameter '" + PARAM_TABLE_KEY + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * convert object array in config object to List<Row>
     *
     * @param aConfig
     * @return data as list of Row objects
     * @throws GenericKnimeSparkException
     */
    public static List<Row> getInputData(final Config aConfig) throws GenericKnimeSparkException {
        final List<Class<?>> types = getColumnTypes(aConfig);

        try {
            final ConfigList data = aConfig.getList(PARAM_DATA_ARRAY);
            return getInputData(types, data);
        } catch (ConfigException e) {
            throw new GenericKnimeSparkException("Input parameter '" + PARAM_DATA_ARRAY
                + "' is not of expected type 'Object[][]': " + e.getMessage());
        }
    }

    /**
     * @param aTypes
     * @param aData
     * @return data as list of Row objects
     */
    public static List<Row> getInputData(final List<Class<?>> aTypes, final ConfigList aData) {
        final List<Row> rows = new ArrayList<>();
        for (ConfigValue rowData : aData) {
            RowBuilder row = RowBuilder.emptyRow();
            ConfigList s = (ConfigList)rowData;
            int ix = 0;
            for (ConfigValue cell : s) {
                if (cell.valueType() == ConfigValueType.NULL) {
                    row.add(null);
                } else {
                    final String v = cell.unwrapped().toString();
                    if (aTypes.get(ix).equals(Integer.class)) {
                        row.add(Integer.valueOf(v));
                    } else if (aTypes.get(ix).equals(Double.class)) {
                        row.add(Double.valueOf(v));
                    } else if (aTypes.get(ix).equals(Boolean.class)) {
                        row.add(Boolean.valueOf(v));
                    } else if (aTypes.get(ix).equals(String.class)) {
                        try {
                            row.add(URLDecoder.decode(v, "UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            row.add(null);
                        }
                    }
                }
                ix++;
            }
            rows.add(row.build());
        }
        return rows;
    }

    /**
     * @param aConfig
     * @return
     * @throws GenericKnimeSparkException
     */
    private static List<Class<?>> getColumnTypes(final Config aConfig) throws GenericKnimeSparkException {
        try {
            ConfigList types = aConfig.getList(PARAM_JAVA_TYPES);
            List<Class<?>> columnTypes = new ArrayList<>();
            for (ConfigValue type : types) {
                final Object o = type.unwrapped();
                if (o.equals("class java.lang.Integer")) {
                    columnTypes.add(Integer.class);
                } else if (o.equals("class java.lang.Double") || o.equals("class java.lang.Float")) {
                    columnTypes.add(Double.class);
                } else if (o.equals("class java.lang.Boolean")) {
                    columnTypes.add(Boolean.class);
                } else if (o.equals("class java.lang.String")) {
                    columnTypes.add(String.class);
                }
            }
            return columnTypes;
        } catch (ConfigException e) {
            throw new GenericKnimeSparkException("Input parameter '" + PARAM_JAVA_TYPES
                + "' is not of expected type 'Object[]'.");
        }
    }

    /**
     * run the actual job, the result is serialized back to the client the true result is stored in the map of named
     * RDDs
     *
     * @return "OK"
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        LOGGER.log(Level.INFO, "inserting KNIME data table into RDD...");
        final List<Row> rowData = getInputData(aConfig);
        storeInRdd(sc, rowData, aConfig.getString(PARAM_TABLE_KEY));
        return JobResult.emptyJobResult().withMessage("OK");
    }

    /**
     * stores the data in a RDD
     *
     *
     * @param aSparkContext
     * @param aData
     * @param aRddName
     */
    private void storeInRdd(final SparkContext aSparkContext, final List<Row> aData, final String aRddName) {
        @SuppressWarnings("resource")
        JavaSparkContext javaContext = new JavaSparkContext(aSparkContext);

        JavaRDD<Row> rdd = javaContext.parallelize(aData);
        LOGGER.log(Level.INFO, "Storing data rdd under key: " + aRddName);
        addToNamedRdds(aRddName, rdd);
    }
}
