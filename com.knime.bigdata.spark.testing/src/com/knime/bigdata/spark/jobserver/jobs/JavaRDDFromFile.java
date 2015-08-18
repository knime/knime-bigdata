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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;

/**
 * converts a text file that is read from disk to a JavaRDD
 * (please note that this does not work unless the driver executes the job which cannot be guaranteed....
 * 
 * @author dwk
 */
public class JavaRDDFromFile extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String PARAM_CSV_SEPARATOR = ParameterConstants.PARAM_SEPARATOR;

    private final static Logger LOGGER = Logger.getLogger(JavaRDDFromFile.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig config) {
        String msg = null;

        if (!config.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg == null && !config.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        // further checks - in this case we check whether the input data file
        // exists
        final String fName = aConfig.getInputParameter(PARAM_INPUT_TABLE);
        if (!new File(fName).exists()) {
            msg =
                "Input data file " + new File(fName).getAbsolutePath()
                    + " does not exist!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    private static String getSeparator(final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_CSV_SEPARATOR)) {
            return aConfig.getInputParameter(PARAM_CSV_SEPARATOR);
        }
        return " ";
    }

    /**
     * run the actual job, the result is serialized back to the client the true result is stored in the map of named
     * RDDs
     *
     * @return "OK"
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig) throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "reading and converting text file...");
        final JavaRDD<Row> parsedData = javaRDDFromFile(sc, aConfig, getSeparator(aConfig));
        LOGGER.log(Level.INFO, "done");

        LOGGER.log(Level.INFO, "Storing parsed data under key: " + aConfig.getOutputStringParameter(PARAM_RESULT_TABLE));
        LOGGER.log(Level.INFO, "Cashing data of size: " + parsedData.count());
        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), parsedData);
        try {
            final StructType schema = StructTypeBuilder.fromRows(parsedData.take(10)).build();
            return JobResult.emptyJobResult().withMessage("OK")
                .withTable(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), schema);
        } catch (InvalidSchemaException e) {
            throw new GenericKnimeSparkException(e);
        }
    }

    static JavaRDD<Row> javaRDDFromFile(final SparkContext sc, final JobConfig config, final String aSeparator) {
        @SuppressWarnings("resource")
        JavaSparkContext ctx = new JavaSparkContext(sc);
        String fName = config.getInputParameter(PARAM_INPUT_TABLE);

        final Function<String, Row> rowFunction = new Function<String, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final String aLine) {
                String[] terms = aLine.split(aSeparator);
                final ArrayList<Object> vals = new ArrayList<>();
                //TODO - we need the column types here
                for (int i = 0; i < terms.length; i++) {
                    //vals.add(Double.parseDouble(terms[i]));
                    vals.add(terms[i]);
                }
                return RowBuilder.emptyRow().addAll(vals).build();
            }
        };
        final JavaRDD<Row> parsedData = ctx.textFile(fName, 1).map(rowFunction);
        return parsedData;
    }
}
