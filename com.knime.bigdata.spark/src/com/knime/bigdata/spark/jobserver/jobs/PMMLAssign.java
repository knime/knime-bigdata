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
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;

/**
 * applies a compiled pmml model to the input data
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLAssign extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_MODEL = ParameterConstants.PARAM_MODEL_NAME;

    /**
     * boolean that indicates if probabilities should be added
     */
    public static final String PARAM_APPEND_PROBABILITIES = "appendProbabilities";

    private static final String PARAM_MAIN_CLASS = ParameterConstants.PARAM_MAIN_CLASS;

    private final static Logger LOGGER = Logger.getLogger(PMMLAssign.class.getName());

    /**
     * parse parameters
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkSelectedColumnIdsParameter(aConfig);
        }

        if (msg == null && !aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }
        if (msg == null && !aConfig.hasInputParameter(PARAM_APPEND_PROBABILITIES)) {
            msg = "Append probabilities missing!";
        }
        if (msg == null && !aConfig.hasInputParameter(PARAM_MAIN_CLASS)) {
            msg = "Main class name missing!";
        }
        if (msg == null && !aConfig.hasInputParameter(PARAM_MODEL)) {
            msg = "Compiled PMML model missing!";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        if (!validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE))) {
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
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.FINE, "starting pmml prediction job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final List<Integer> colIdxs = SupervisedLearnerUtils.getSelectedColumnIds(aConfig);
        final boolean addProbabilites = aConfig.getInputParameter(PARAM_APPEND_PROBABILITIES, Boolean.class);
        final String mainClass = aConfig.getInputParameter(PARAM_MAIN_CLASS);
        final Map<String, byte[]> bytecode = aConfig.decodeFromInputParameter(PARAM_MODEL);
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
                        final Integer colIdx = colIdxs.get(i);
                        if (colIdx == null || colIdx < 0) {
                            in[i] = null;
                        } else {
                            in[i] = r.get(colIdx);
                        }
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
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), predicted);
            LOGGER.log(Level.FINE, "pmml prediction done");
            return JobResult.emptyJobResult().withMessage("OK")
                .withTable(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), null);
        } catch (Exception e) {
            final String msg = "Exception in pmml prediction job: " + e.getMessage();
            LOGGER.log(Level.SEVERE, msg, e);
            throw new GenericKnimeSparkException(msg, e);
        }
    }
}
