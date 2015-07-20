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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * applies a compiled pmml model to the input data
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLAssign extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_DATA_FILE_NAME = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_MODEL = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_MODEL_NAME;

    private static final String PARAM_COL_IDXS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_COL_IDXS;

    private static final String PARAM_PROBS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_APPEND_PROBABILITIES;

    private static final String PARAM_MAIN_CLASS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_MAIN_CLASS;

    private static final String PARAM_OUTPUT_DATA_PATH = ParameterConstants.PARAM_OUTPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(PMMLAssign.class.getName());

    /**
     * parse parameters
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;
        if (!aConfig.hasPath(PARAM_DATA_FILE_NAME)) {
            msg = "Input parameter '" + PARAM_DATA_FILE_NAME + "' missing.";
        }

        if (msg == null) {
            if (!aConfig.hasPath(PARAM_COL_IDXS)) {
                msg = "Input parameter '" + PARAM_COL_IDXS + "' missing.";
            } else {
                try {
                    aConfig.getIntList(PARAM_COL_IDXS);
                } catch (ConfigException e) {
                    msg = "Input parameter '" + PARAM_COL_IDXS + "' is not of expected type 'integer list'.";
                }
            }
        }

        if (msg == null && !aConfig.hasPath(PARAM_OUTPUT_DATA_PATH)) {
            msg = "Output parameter '" + PARAM_OUTPUT_DATA_PATH + "' missing.";
        }
        if (msg == null && !aConfig.hasPath(PARAM_PROBS)) {
            msg = "Append probabilities missing!";
        }
        if (msg == null && !aConfig.hasPath(PARAM_MAIN_CLASS)) {
            msg = "Main class name missing!";
        }
        if (msg == null && !aConfig.hasPath(PARAM_MODEL)) {
            msg = "Compiled PMML model missing!";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final Config aConfig) throws GenericKnimeSparkException {
        String msg = null;
        if (!validateNamedRdd(aConfig.getString(PARAM_DATA_FILE_NAME))) {
            msg = "Input data table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @return "OK" - the actual predictions are stored in a named rdd and can be retrieved by a separate job or used
     *         later on
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.FINE, "starting pmml prediction job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getString(PARAM_DATA_FILE_NAME));
        final List<Integer> colIdxs = aConfig.getIntList(PARAM_COL_IDXS);
        final boolean addProbabilites = aConfig.getBoolean(PARAM_PROBS);
        final String mainClass = aConfig.getString(PARAM_MAIN_CLASS);
        final Map<String, byte[]> bytecode = ModelUtils.fromString(aConfig.getString(PARAM_MODEL));
        try {
            final Function<Row, Row> predict = new Function<Row, Row>() {
                private static final long serialVersionUID = 1L;

                //use transient since a Method can not be serialized
                private transient Method m_evalMethod;

                /** {@inheritDoc} */
                @Override
                public Row call(final Row r) throws Exception {
                    if (m_evalMethod == null) {
                        final ClassLoader cl = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
                            /** {@inheritDoc} */
                            @Override
                            protected Class<?> findClass(final String name) throws ClassNotFoundException {
                                byte[] bc = bytecode.get(name);
                                return defineClass(name, bc, 0, bc.length);
                            }
                        };
                        final Class<?> modelClass = cl.loadClass(mainClass);
                        m_evalMethod = modelClass.getMethod("evaluate", Object[].class);
                    }
                    final Object[] in = new Object[colIdxs.size()];
                    for (int i = 0; i < colIdxs.size(); i++) {
                        in[i] = r.get(colIdxs.get(i));
                    }
                    final Object[] result = (Object[])m_evalMethod.invoke(null, (Object)in);

                    final RowBuilder rowBuilder = RowBuilder.fromRow(r);
                    if (addProbabilites) {
                        return rowBuilder.addAll(Arrays.asList(result)).build();
                    }
                    return rowBuilder.add(result[0]).build();
                }
            };
            final JavaRDD<Row> predicted = rowRDD.map(predict);
            addToNamedRdds(aConfig.getString(PARAM_OUTPUT_DATA_PATH), predicted);
            LOGGER.log(Level.FINE, "pmml prediction done");
            return JobResult.emptyJobResult().withMessage("OK")
                .withTable(aConfig.getString(PARAM_OUTPUT_DATA_PATH), null);
        } catch (Exception e) {
            final String msg = "Exception in pmml prediction job: " + e.getMessage();
            LOGGER.log(Level.SEVERE, msg, e);
            throw new GenericKnimeSparkException(msg, e);
        }
    }
}
