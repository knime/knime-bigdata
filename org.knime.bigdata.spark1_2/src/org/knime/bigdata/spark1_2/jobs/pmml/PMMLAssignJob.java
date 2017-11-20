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
import java.io.ObjectInputStream;
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
 *
 * @author Tobias Koetter, KNIME.com
 * @param <I> the {@link PMMLAssignJobInput}
 */
@SparkClass
public abstract class PMMLAssignJob<I extends PMMLAssignJobInput> implements SparkJobWithFiles<I, EmptyJobOutput> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(PMMLAssignJob.class.getName());

    /**
     *
     */
    public PMMLAssignJob() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final I input,
        final List<File> inputFiles, final NamedObjects namedObjects) throws KNIMESparkException, Exception {
     LOGGER.info("starting pmml asignment job...");
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        final List<Integer> inputColIdxs = input.getColumnIdxs();
        final String mainClass = input.getMainClass();
        File file = inputFiles.get(0);
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            @SuppressWarnings("unchecked")
            final Map<String, byte[]> bytecode = (Map<String, byte[]>)in.readObject();
            try {
                Function<Row, Row> asign = createFunction(bytecode, mainClass, inputColIdxs, input);
                final JavaRDD<Row> resultRDD = rowRDD.map(asign);
                namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), resultRDD);
                LOGGER.info("pmml asigment done");
                return EmptyJobOutput.getInstance();
            } catch (Exception e) {
                final String msg = "Exception in pmml asignment job: " + e.getMessage();
                LOGGER.error(msg, e);
                throw new KNIMESparkException(msg, e);
            }
        }
    }

    /**
     * @param inputColIdxs
     * @param mainClass
     * @param bytecode
     * @param aConfig
     * @return the pre
     */
    protected abstract Function<Row, Row> createFunction(final Map<String, byte[]> bytecode, final String mainClass,
        final List<Integer> inputColIdxs, I input);
}