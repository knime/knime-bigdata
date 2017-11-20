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
package com.knime.bigdata.spark.core.jobserver;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.knime.bigdata.spark.core.job.JobData;
import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class JobserverJobInput extends JobData {

    private static final String JOBSERVER_PREFIX = "js";

    private static final String KEY_JOBINPUT_CLASS = "jobInputClass";

    private static final String KEY_JOB_CLASS = "jobClass";

    private static final String KEY_LOG4JLOG_LEVEL = "loglevel";

    private static final String KEY_FILES = "inputFiles";

    JobserverJobInput() {
        super(JOBSERVER_PREFIX);
    }

    JobserverJobInput(final Map<String,Object> internalMap) {
        super(JOBSERVER_PREFIX, internalMap);
    }

    public int getLog4jLogLevel() {
        return getInteger(KEY_LOG4JLOG_LEVEL);
    }

    public String getSparkJobClass() {
        return get(KEY_JOB_CLASS);
    }

    /**
     * Instantiates and returns the wrapped {@link JobInput} backed by the same internal map.
     *
     * @return The wrapped spark job input
     *
     * @throws ClassNotFoundException, InstantiationException, IllegalAccessException If something went wrong during
     *             instantiation.
     */
    @SuppressWarnings("unchecked")
    public <T extends JobInput> T getSparkJobInput()
        throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        final String jobInputClassName = get(KEY_JOBINPUT_CLASS);
        final T jobInput = (T)getClass().getClassLoader().loadClass(jobInputClassName).newInstance();
        jobInput.setInternalMap(getInternalMap());

        return jobInput;
    }

    public boolean isJobWithFiles() {
        return has(KEY_FILES);
    }

    public List<String> getFiles() {
        return getOrDefault(KEY_FILES, Collections.<String>emptyList());
    }

    public static JobserverJobInput createFromSparkJobInput(final JobInput jobInput,
        final String sparkJobClass) {
        JobserverJobInput jsInput = new JobserverJobInput(jobInput.getInternalMap());
        jsInput.set(KEY_JOBINPUT_CLASS, jobInput.getClass().getName());
        jsInput.set(KEY_JOB_CLASS, sparkJobClass);
        return jsInput;
    }

    public static JobserverJobInput createFromMap(final Map<String, Object> internalMap) {
        JobserverJobInput jsInput = new JobserverJobInput(internalMap);
        return jsInput;
    }

    public JobserverJobInput withLog4jLogLevel(final int log4jLogLevel) {
        set(KEY_LOG4JLOG_LEVEL, log4jLogLevel);
        return this;
    }

    public JobserverJobInput withFiles(final List<String> serverFilenames) {
        set(KEY_FILES, serverFilenames);
        return this;
    }
}
