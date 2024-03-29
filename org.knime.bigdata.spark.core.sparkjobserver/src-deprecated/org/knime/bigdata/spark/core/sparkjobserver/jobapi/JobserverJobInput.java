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
package org.knime.bigdata.spark.core.sparkjobserver.jobapi;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.knime.bigdata.spark.core.job.JobData;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

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

    private List<String> m_jobServerFiles;

    JobserverJobInput() {
        super(JOBSERVER_PREFIX);
    }

    JobserverJobInput(final Map<String, Object> internalMap) {
        super(JOBSERVER_PREFIX, internalMap);
    }

    /**
     * @return the log level to use in Spark for the job
     */
    public int getLog4jLogLevel() {
        return getInteger(KEY_LOG4JLOG_LEVEL);
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
     * @param jobInput the {@link JobInput}
     * @param sparkJobClass the Spark job class to use
     * @return the {@link JobserverJobInput}
     */
    public static JobserverJobInput createFromSparkJobInput(final JobInput jobInput, final String sparkJobClass) {
        final JobserverJobInput jsInput = new JobserverJobInput(jobInput.getInternalMap());
        jsInput.set(KEY_JOBINPUT_CLASS, jobInput.getClass().getName());
        jsInput.set(KEY_JOB_CLASS, sparkJobClass);
        return jsInput;
    }

    /**
     * @param internalMap of parameters
     * @return the {@link JobserverJobInput}
     */
    public static JobserverJobInput createFromMap(final Map<String, Object> internalMap) {
        final JobserverJobInput jsInput = new JobserverJobInput(internalMap);
        return jsInput;
    }

    /**
     * @param log4jLogLevel the log level to use
     * @return the {@link JobserverJobInput} itself
     */
    public JobserverJobInput withLog4jLogLevel(final int log4jLogLevel) {
        set(KEY_LOG4JLOG_LEVEL, log4jLogLevel);
        return this;
    }

    /**
     * Adds the given path string of a server file to the list of files.
     *
     * @param path a string representing the path to a file on the server
     * @return this object, with the given file added
     */
    @SuppressWarnings("unchecked")
    public <T extends JobData> T withJobServerFile(final String path) {
        if (m_jobServerFiles == null) {
            m_jobServerFiles = new LinkedList<>();
        }

        m_jobServerFiles.add(path);
        return (T)this;
    }

    /**
     * @return a list of string representing path on the server
     */
    public List<String> getJobServerFiles() {
        if (m_jobServerFiles != null) {
            return Collections.unmodifiableList(m_jobServerFiles);
        } else {
            return Collections.emptyList();
        }
    }
}
