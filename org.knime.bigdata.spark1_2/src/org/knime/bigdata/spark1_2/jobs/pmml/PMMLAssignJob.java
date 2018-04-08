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
 *   Created on 29.09.2015 by koetter
 */
package org.knime.bigdata.spark1_2.jobs.pmml;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.pmml.PMMLAssignJobInput;
import org.knime.bigdata.spark1_2.api.NamedObjects;
import org.knime.bigdata.spark1_2.api.SparkJobWithFiles;

/**
 * Abstract super class of jobs that use compiled PMML for assignment (e.g. PMML transformations or predictions).
 *
 * @author Tobias Koetter, KNIME.com
 * @param <I> the {@link PMMLAssignJobInput}
 */
@SparkClass
public abstract class PMMLAssignJob<I extends PMMLAssignJobInput> implements SparkJobWithFiles<I, EmptyJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PMMLAssignJob.class.getName());

    @SuppressWarnings("unchecked")
    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final I input,
        final List<File> inputFiles, final NamedObjects namedObjects) throws KNIMESparkException {

        LOGGER.info("Starting PMML Assignment job...");
        final JavaRDD<Row> inputRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());

        final Map<String, byte[]> bytecode;
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(inputFiles.get(0))))) {
            bytecode = (Map<String, byte[]>)in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new KNIMESparkException("Failed to load compiled PMML", e);
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        final Function<Row, Row> assignMapFunction = createMapFunction(bytecode, input);
        final JavaRDD<Row> resultRDD = inputRDD.map(assignMapFunction);
        namedObjects.addJavaRdd(namedOutputObject, resultRDD);

        LOGGER.info("PMML assigment done");
        return EmptyJobOutput.getInstance();
    }

    /**
     * Utility method that loads the given class from the given bytecode and returns its "evaluate" method.
     *
     * @param bytecode Compiled PMML byte code (maps class names to byte code).
     * @param mainClass The class from which to take the the "evaluate" method.
     * @return the "evaluate" method.
     * @throws ClassNotFoundException If the given class was not found.
     * @throws NoSuchMethodException If the given class did not provide an "evaluate" method.
     */
    protected Method loadCompiledPMMLEvalMethod(final Map<String, byte[]> bytecode, final String mainClass)
        throws ClassNotFoundException, NoSuchMethodException {
        final ClassLoader cl = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
            /** {@inheritDoc} */
            @Override
            protected Class<?> findClass(final String name) throws ClassNotFoundException {
                byte[] bc = bytecode.get(name);
                return defineClass(name, bc, 0, bc.length);
            }
        };
        final Class<?> modelClass = cl.loadClass(mainClass);
        return modelClass.getMethod("evaluate", Object[].class);
    }

    /**
     * Creates a {@link Function} that can be applied on the ingoing RDD. To be defined by subclasses.
     *
     * @param bytecode Compiled PMML byte code (maps class names to byte code).
     * @param input The job input.
     * @return PMML assign function as a {@link Function} that can be applied on the ingoing RDD.
     */
    protected abstract Function<Row, Row> createMapFunction(Map<String, byte[]> bytecode, I input);

    /**
     * Fills the given array with values from the given row.
     *
     * @param rowColumnIdxs Specifies column indices of the row that shall be copied over. List must have as many
     *            elements as the given array.
     * @param row The row object that provides the values to copy.
     * @param evalMethodInput The array to copy the row values to.
     * @param longColumns <code>true</code> if PMML method input is a long value (for each method input field)
     */
    protected void fillEvalMethodInputFromRow(final ArrayList<Integer> rowColumnIdxs, final Row row, final Object[] evalMethodInput,
        final boolean[] longColumns) {

        for (int i = 0; i < evalMethodInput.length; i++) {
            final Integer colIdx = rowColumnIdxs.get(i);
            if (colIdx == null || colIdx < 0) {
                evalMethodInput[i] = null;
            } else {
                evalMethodInput[i] = row.get(colIdx);

                // PMML dictionary bug workaround (remove this if dictionary support longs):
                if (longColumns[i] && evalMethodInput[i] != null) {
                    evalMethodInput[i] = ((Long) evalMethodInput[i]).doubleValue();
                }
            }
        }
    }

}