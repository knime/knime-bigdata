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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;
import org.knime.base.pmml.translation.java.compile.CompiledModel;
import org.knime.base.pmml.translation.java.compile.CompiledModel.MiningFunction;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.pmml.predictor.SerializableCompiledModel;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * applies previously learned MLlib KMeans model to given RDD, predictions are inserted into a new RDD and (temporarily)
 * stored in the map of named RDDs, optionally saved to disk
 *
 * @author koetter, dwk
 */
public class PMMLAssign extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_DATA_FILE_NAME = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_MODEL = ParameterConstants.PARAM_INPUT + "."
            + ParameterConstants.PARAM_MODEL_NAME;

    private static final String PARAM_COL_IDXS = ParameterConstants.PARAM_INPUT
            + "." + ParameterConstants.PARAM_COL_IDXS;

    private static final String PARAM_PROBS = ParameterConstants.PARAM_INPUT + "."
            + ParameterConstants.PARAM_APPEND_PROBABILITIES;

    private static final String PARAM_OUTPUT_DATA_PATH = ParameterConstants.PARAM_OUTPUT + "."
            + ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(PMMLAssign.class.getName());

    /**
     * parse parameters - there are no default values, but two required values: - the kmeans model - the input JavaRDD
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;
        if (!aConfig.hasPath(PARAM_DATA_FILE_NAME)) {
            msg = "Input parameter '" + PARAM_DATA_FILE_NAME + "' missing.";
        }

        if (msg == null && !aConfig.hasPath(PARAM_COL_IDXS)) {
            msg = "Input parameter '" + PARAM_COL_IDXS + "' missing.";
        } else {
            try {
                aConfig.getIntList(PARAM_COL_IDXS);
            } catch (ConfigException e) {
                msg = "Input parameter '" + PARAM_COL_IDXS
                        + "' is not of expected type 'integer list'.";
            }
        }

        if (msg == null && !aConfig.hasPath(PARAM_OUTPUT_DATA_PATH)) {
            msg = "Output parameter '" + PARAM_OUTPUT_DATA_PATH + "' missing.";
        }
        if (msg == null && !aConfig.hasPath(PARAM_PROBS)) {
            msg = "Append probabilities missing!";
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

        LOGGER.log(Level.INFO, "starting pmml prediction job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getString(PARAM_DATA_FILE_NAME));
        final List<Integer> colIdxs = aConfig.getIntList(PARAM_COL_IDXS);

        final Map<String,byte[]> bytecode = ModelUtils.fromString(aConfig.getString(PARAM_MODEL));

        final ClassLoader cl = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
                /**
                 * {@inheritDoc}
                 */
                @Override
                protected Class<?> findClass(final String name) throws ClassNotFoundException {
                    byte[] bc = bytecode.get(name);
                    return defineClass(name, bc, 0, bc.length);
                }
            };
            Class<?> modelClass = cl.loadClass("MainModel");
            try {
                //see CompiledModel as an example on how to use it
                Method evalMethod = modelClass.getMethod("evaluate", new Object[0].getClass());
                MiningFunction m_miningFunction = MiningFunction.valueOf(((String)modelClass
                        .getMethod("getMiningFunction", (Class<?>[])null).invoke(null, (Object[])null)).toUpperCase());
                String[] m_outputFields = (String[])modelClass
                        .getMethod("getOutputFields", (Class<?>[])null).invoke(null, (Object[])null);
                String[] m_inputFields = (String[])modelClass
                        .getMethod("getInputFields", (Class<?>[])null).invoke(null, (Object[])null);
                int m_numInputs = (int)modelClass
                        .getMethod("getNumInputs", (Class<?>[])null).invoke(null, (Object[])null);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                    | SecurityException e) {
                throw new GenericKnimeSparkException("The given class is not a compiled model", e);
            }
            try {
                Object[] result = (Object[])evalMethod.invoke(null, ((Object)data));
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new GenericKnimeSparkException("The compiled prediction method could not be executed", e);
            }
        }



        final Function<Row, Row> predict = new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row r) {
                final CompiledModel model = modelS.getCompiledModel();
                Object[] in = new Object[colIdxs.size()];
                for (int i = 0; i < colIdxs.size(); i++) {
                    in[i] = r.get(colIdxs.get(i));
                }
                Object[] result = null;
                try {
                    result = model.evaluate(in);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (result != null) {
                    if (rowRDD.m_copiedCols.length == 0) {
                        return Row.create(result);
                    } else {
                        Object[] output = new Object[result.length + m_copiedCols.length];
                        for (int i = 0; i < m_copiedCols.length; i++) {
                            output[i] = r.get(m_copiedCols[i]);
                        }
                        for (int i = 0; i < result.length; i++) {
                            output[i + m_copiedCols.length] = result[i];
                        }
                        return Row.create(output);
                    }
                }
                return null;
            }
        };
        final JavaRDD<Row> predicted = rowRDD.map(predict);
//        final KMeansModel kMeansModel = ModelUtils.fromString(aConfig.getString(PARAM_MODEL));
//
//        final JavaRDD<Row> predictions = ModelUtils.predict(sc, inputRDD, rowRDD, kMeansModel);

        LOGGER.log(Level.INFO, "pmml prediction done");
        addToNamedRdds(aConfig.getString(PARAM_OUTPUT_DATA_PATH), predicted);
    }
}
