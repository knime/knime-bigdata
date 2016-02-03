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
 *   Created on Jan 26, 2016 by bjoern
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;

import spark.jobserver.SparkJobValidation;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkJavaSnippetJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 6708769732202293469L;

    private final static Logger LOGGER = Logger.getLogger(SparkJavaSnippetJob.class.getName());

    /**
     * first input table
     */
    public static final String PARAM_INPUT_TABLE_KEY1 = PARAM_INPUT_TABLE;

    /**
     * second input table
     */
    public static final String PARAM_INPUT_TABLE_KEY2 = "InputTable2";

    /**
     * output table name
     */
    public static final String PARAM_OUTPUT_TABLE_KEY = PARAM_RESULT_TABLE;

    /**
     * Key under which name of snippet class to be loaded is stored.
     */
    public static final String PARAM_SNIPPET_CLASS = "snippetClass";


    /**
     * Path in local filesystem (of jobserver), to jar file with snippet code that needs to be loaded.
     */
    public static final String PARAM_JAR_FILE_TO_ADD = "jarFileToAdd";

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        if (!aConfig.hasInputParameter(PARAM_SNIPPET_CLASS)) {
            return ValidationResultConverter.invalid("Input parameter '" + PARAM_SNIPPET_CLASS + "' missing.");
        }

        if (!aConfig.hasInputParameter(PARAM_JAR_FILE_TO_ADD)) {
            return ValidationResultConverter.invalid("Input parameter '" + PARAM_JAR_FILE_TO_ADD + "' missing.");
        }

        if (!aConfig.hasOutputParameter(PARAM_OUTPUT_TABLE_KEY)) {
            return ValidationResultConverter.invalid("Output parameter '" + PARAM_OUTPUT_TABLE_KEY + "' missing.");
        }

        return ValidationResultConverter.valid();
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;

        if (aConfig.hasInputParameter(PARAM_INPUT_TABLE_KEY1)
            && !validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY1))) {
            msg = "(First) Input data table missing for key: " + aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY1);

        } else if (aConfig.hasInputParameter(PARAM_INPUT_TABLE_KEY2)
            && !validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY2))) {
            msg = "Second input data table missing for key: " + aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY2);

        } else if (!new File(aConfig.getInputParameter(PARAM_JAR_FILE_TO_ADD)).canRead()) {
            msg = String.format("Cannot read jar file %s. Does it exist?",
                aConfig.getInputParameter(PARAM_JAR_FILE_TO_ADD));
        }

        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ": " + msg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JobResult runJobWithContext(final SparkContext sparkContext, final JobConfig aConfig)
        throws GenericKnimeSparkException {

        LOGGER.log(Level.INFO, "Starting Java Snippet transformation job");
        validateInput(aConfig);

        JavaRDD<Row> rowRDD1 = getNamedRowRDD(aConfig, PARAM_INPUT_TABLE_KEY1);
        JavaRDD<Row> rowRDD2 = getNamedRowRDD(aConfig, PARAM_INPUT_TABLE_KEY2);

        LOGGER.log(Level.INFO, "Adding jar to workers");
        sparkContext.addJar(aConfig.getInputParameter(PARAM_JAR_FILE_TO_ADD));

        addToClassLoader(aConfig.getInputParameter(PARAM_JAR_FILE_TO_ADD), (URLClassLoader)getClass().getClassLoader());

        final AbstractSparkJavaSnippet snippet;

        try {
            final Class<?> snippetClass = getClass().getClassLoader().loadClass
                    (aConfig.getInputParameter(PARAM_SNIPPET_CLASS));
            snippet = (AbstractSparkJavaSnippet)snippetClass.newInstance();
        } catch (Exception e) {
            throw new GenericKnimeSparkException("Could not instantiate Java snippet class. Error: " + e, e);
        }

        JavaRDD<Row> resultRDD = snippet.apply(new JavaSparkContext(sparkContext), rowRDD1, rowRDD2);

        LOGGER.log(Level.INFO, "Completed invocation of Java snippet code");
        if (resultRDD == null) {
            LOGGER.log(Level.FINE, "No result RDD found");
            return JobResult.emptyJobResult().withMessage("OK");
        } else {
            LOGGER.log(Level.INFO, "Getting schema for result RDD");
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_OUTPUT_TABLE_KEY), resultRDD);

            try {
                final StructType schema = snippet.getSchema(resultRDD);
                return JobResult.emptyJobResult().withMessage("OK")
                    .withTable(aConfig.getOutputStringParameter(PARAM_OUTPUT_TABLE_KEY), schema);
            } catch (InvalidSchemaException e) {
                throw new GenericKnimeSparkException("Could not determine result RDD schema. Error: " + e, e);
            }
        }
    }

    private JavaRDD<Row> getNamedRowRDD(final JobConfig aConfig, final String inputTableKey) {
        final JavaRDD<Row> rowRDD;
        if (aConfig.hasInputParameter(inputTableKey)) {
            rowRDD = getFromNamedRdds(aConfig.getInputParameter(inputTableKey));
        } else {
            rowRDD = null;
        }
        return rowRDD;
    }


    private void addToClassLoader(final String filename, final URLClassLoader classLoader) throws GenericKnimeSparkException {

        try {
            LOGGER.log(Level.INFO, "Searching for addURL method");
            Method addURLMethod = findMethod(classLoader.getClass(), "addURL", URL.class);
            addURLMethod.setAccessible(true);
            LOGGER.log(Level.INFO, "Invoking addURL method");
            addURLMethod.invoke(classLoader, new URL("file:" + filename));
        } catch (Exception e) {
            throw new GenericKnimeSparkException(e.getMessage(), e);
        }
    }

    private Method findMethod(final Class<?> clazz, final String methodName, final Class<?>... parameterTypes)
        throws NoSuchMethodException {
        Class<?> currClass = clazz;

        while (currClass != null) {
            try {
                LOGGER.log(Level.INFO, "Trying to get addURL from " + currClass.getCanonicalName());
                return currClass.getDeclaredMethod(methodName, parameterTypes);
            } catch (NoSuchMethodException e) {
                currClass = clazz.getSuperclass();
            }
        }

        throw new NoSuchMethodException("Could not find method: " + methodName);
    }
}
