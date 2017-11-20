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
 *
 * History
 *   Created on Jan 26, 2016 by bjoern
 */
package org.knime.bigdata.spark1_3.jobs.scripting.java;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobInput;
import org.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobOutput;
import org.knime.bigdata.spark1_3.api.NamedObjects;
import org.knime.bigdata.spark1_3.api.SparkJobWithFiles;
import org.knime.bigdata.spark1_3.api.TypeConverters;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class JavaSnippetJob implements SparkJobWithFiles<JavaSnippetJobInput, JavaSnippetJobOutput> {

    private static final long serialVersionUID = 6708769732202293469L;

    private final static Logger LOGGER = Logger.getLogger(JavaSnippetJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public JavaSnippetJobOutput runJob(final SparkContext sparkContext, final JavaSnippetJobInput input,
        final List<File> jarFiles, final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        JavaRDD<Row> rowRDD1 = getRowRDD(namedObjects, input.getNamedInputObjects(), 0);
        JavaRDD<Row> rowRDD2 = getRowRDD(namedObjects, input.getNamedInputObjects(), 1);

        JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(jarFiles);

        final AbstractSparkJavaSnippet snippet;

        try {
            final Class<?> snippetClass = getClass().getClassLoader().loadClass(input.getSnippetClass());
            snippet = (AbstractSparkJavaSnippet)snippetClass.newInstance();
        } catch (Exception e) {
            throw new KNIMESparkException("Could not instantiate snippet class. Error: " + e.getMessage(), e);
        }

        setFlowVariableValues(snippet, input.getFlowVariableValues());

        JavaRDD<Row> resultRDD = snippet.apply(new JavaSparkContext(sparkContext), rowRDD1, rowRDD2);

        if (!(snippet instanceof AbstractSparkJavaSnippetSink) && resultRDD == null) {
            throw new KNIMESparkException("Snippet must not return a null reference!");
        }

        LOGGER.log(Level.INFO, "Completed execution of Java snippet code");

        if (!input.getNamedOutputObjects().isEmpty()) {
            if (resultRDD != null) {
                namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), resultRDD);
                LOGGER.log(Level.INFO, "Getting schema for result RDD");
                return new JavaSnippetJobOutput(input.getFirstNamedOutputObject(),
                    TypeConverters.convertSpec(snippet.getSchema(resultRDD)));
            } else {
                // this is most likely an error in the snippet code, hence we use a KNIMESparkException
                throw new KNIMESparkException("Snippet must return an RDD");
            }
        } else {
            return new JavaSnippetJobOutput();
        }
    }


    private JavaRDD<Row> getRowRDD(final NamedObjects namedObjects, final List<String> namedObjectsList, final int i) {
        if (namedObjectsList.size() > i) {
            return namedObjects.getJavaRdd(namedObjectsList.get(i));
        } else {
            return null;
        }
    }

    private void setFlowVariableValues(final AbstractSparkJavaSnippet snippet,
        final Map<String, Object> decodeFromInputParameter) throws KNIMESparkException {

        for (String javaFieldName : decodeFromInputParameter.keySet()) {
            try {
                Field field = snippet.getClass().getDeclaredField(javaFieldName);
                field.setAccessible(true);
                field.set(snippet, decodeFromInputParameter.get(javaFieldName));
            } catch (Exception e) {
                // this is most likely an error in the snippet code, hence we use a KNIMESparkException
                throw new KNIMESparkException(String.format(
                    "Error setting member field %s in instance of Java snippet: %s", javaFieldName, e.getMessage()), e);
            }
        }
    }
}
