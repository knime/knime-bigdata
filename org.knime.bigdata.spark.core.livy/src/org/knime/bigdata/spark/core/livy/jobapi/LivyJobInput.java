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

import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    private static final String KEY_FILES = "inputFiles";

    LivyJobInput() {
        super(LIVY_PREFIX);
    }

    LivyJobInput(final Map<String, Object> internalMap) {
        super(LIVY_PREFIX, internalMap);
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

        return jobInput;
    }

    /**
     * @return <code>true</code> if the job input contains files
     * @see #getFiles()
     */
    public boolean isJobWithFiles() {
        return has(KEY_FILES);
    }

    /**
     * @return gets the names of files in the staging area for the job.
     * @see #isJobWithFiles()
     */
    public List<String> getFiles() {
        return getOrDefault(KEY_FILES, Collections.<String>emptyList());
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
    	LivyJobInput jsInput = new LivyJobInput(jobInput.getInternalMap());
        jsInput.set(KEY_JOBINPUT_CLASS, jobInput.getClass().getName());
        jsInput.set(KEY_JOB_CLASS, sparkJobClass);
        return jsInput;
    }

    /**
     * Creates a new {@link LivyJobInput} backed by the given map.
     * 
     * @param internalMap of parameters
     * @return the {@link LivyJobInput}
     */
    public static LivyJobInput createFromMap(final Map<String, Object> internalMap) {
    	LivyJobInput jsInput = new LivyJobInput(internalMap);
        return jsInput;
    }

    /**
     * Adds a list of files to be used by the job.
     * 
     * @param serverFilenames the path to the files.
     * @return the {@link LivyJobInput} itself
     */
    public LivyJobInput withFiles(final List<String> serverFilenames) {
        set(KEY_FILES, serverFilenames);
        return this;
    }
}
