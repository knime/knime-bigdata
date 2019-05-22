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
 *   Created on Apr 11, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.livy.jobapi;

import java.nio.file.Path;

import org.knime.bigdata.spark.core.job.JobData;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * A class that wraps that wraps the actual job input for Spark jobs on Livy.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class LivyJobInput extends JobData {

    private static final String LIVY_PREFIX = "livy";

    private static final String KEY_JOBINPUT_CLASS = "jobInputClass";

    private static final String KEY_JOB_CLASS = "jobClass";

    /**
     * Empty constructor for (de)serialization.
     */
    public LivyJobInput() {
        super(LIVY_PREFIX);
    }

    LivyJobInput(final JobInput jobInput, final String sparkJobClass) {
        super(LIVY_PREFIX, jobInput.getInternalMap());
        set(KEY_JOBINPUT_CLASS, jobInput.getClass().getCanonicalName());
        set(KEY_JOB_CLASS, sparkJobClass);
        for (Path inputFile : jobInput.getFiles()) {
            withFile(inputFile);
        }
    }

    /**
     * @return the Spark job class name
     */
    public String getSparkJobClass() {
        return get(KEY_JOB_CLASS);
    }

    /**
     * Instantiates and returns the wrapped {@link JobInput} backed by the same internal map.
     *
     * @return The wrapped spark job input
     *
     * @throws ClassNotFoundException If something went wrong during instantiation.
     * @throws InstantiationException If something went wrong during instantiation.
     * @throws IllegalAccessException If something went wrong during instantiation.
     */
    @SuppressWarnings("unchecked")
    public <T extends JobInput> T getSparkJobInput()
        throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        final String jobInputClassName = get(KEY_JOBINPUT_CLASS);
        final T jobInput = (T)getClass().getClassLoader().loadClass(jobInputClassName).newInstance();

        jobInput.setInternalMap(getInternalMap());
        for (Path inputFile : getFiles()) {
            jobInput.withFile(inputFile);
        }

        return jobInput;
    }

    /**
     * Wraps the given job input into an instance of {@link LivyJobInput}.
     * 
     * @param jobInput the {@link JobInput}
     * @param sparkJobClass the Spark job class to use
     * @return the {@link LivyJobInput}
     */
    public static LivyJobInput createFromSparkJobInput(final JobInput jobInput,
        final String sparkJobClass) {
        return new LivyJobInput(jobInput, sparkJobClass);
    }
}
