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
 *   Created on Aug 17, 2017 by bjoern
 */
package org.knime.bigdata.spark2_3.base;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobInput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobSerializationUtils;
import org.knime.bigdata.spark2_3.api.SimpleSparkJob;
import org.knime.bigdata.spark2_3.api.SparkJob;
import org.knime.bigdata.spark2_3.api.SparkJobWithFiles;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class LivySparkJob implements Job<LivyJobOutput> {

    private static final long serialVersionUID = 1L;

    private final LivyJobInput m_livyInput;

    /**
     * Public constructor invoked by Livy.
     *
     * @param input The deserialized job input.
     */
    public LivySparkJob(final LivyJobInput input) {
        m_livyInput = input;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public LivyJobOutput call(final JobContext ctx) throws Exception {
        LivyJobOutput toReturn;

        try {
            m_livyInput.setInternalMap(LivyJobSerializationUtils.postKryoDeserialize(m_livyInput.getInternalMap(),
                getClass().getClassLoader()));
            final JobInput jobInput = m_livyInput.getSparkJobInput();

            NamedObjectsImpl.ensureNamedInputObjectsExist(jobInput);
            NamedObjectsImpl.ensureNamedOutputObjectsDoNotExist(jobInput);
            List<File> inputFiles = validateInputFiles();

            Object sparkJob = getClass().getClassLoader().loadClass(m_livyInput.getSparkJobClass()).newInstance();

            final SparkContext sc = ctx.sc().sc();

            if (sparkJob instanceof SparkJob) {
                toReturn = LivyJobOutput
                    .success(((SparkJob)sparkJob).runJob(sc, jobInput, NamedObjectsImpl.SINGLETON_INSTANCE));
            } else if (sparkJob instanceof SparkJobWithFiles) {
                toReturn = LivyJobOutput.success(((SparkJobWithFiles)sparkJob).runJob(sc, jobInput, inputFiles,
                    NamedObjectsImpl.SINGLETON_INSTANCE));
            } else {
                ((SimpleSparkJob)sparkJob).runJob(sc, jobInput, NamedObjectsImpl.SINGLETON_INSTANCE);
                toReturn = LivyJobOutput.success();
            }
        } catch (KNIMESparkException e) {
            toReturn = LivyJobOutput.failure(e);
        } catch (Throwable t) {
            toReturn =
                LivyJobOutput.failure(new KNIMESparkException("Failed to execute Spark job: " + t.getMessage(), t));
        }

        return LivyJobOutput.fromMap(LivyJobSerializationUtils.preKryoSerialize(toReturn.getInternalMap()));
    }

    private List<File> validateInputFiles() throws KNIMESparkException {
        final List<File> inputFiles = new LinkedList<>();

        for (String filename : m_livyInput.getFiles()) {
            File inputFile = new File(SparkFiles.get(filename));
            if (inputFile.canRead()) {
                inputFiles.add(inputFile);
            } else {
                throw new KNIMESparkException("Cannot read job input file on driver: " + filename);
            }
        }

        return inputFiles;
    }
}
