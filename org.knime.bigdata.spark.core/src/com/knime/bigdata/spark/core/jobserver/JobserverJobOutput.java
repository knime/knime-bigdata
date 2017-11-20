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
 *   Created on 26.05.2015 by dwk
 */
package com.knime.bigdata.spark.core.jobserver;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.knime.bigdata.spark.core.job.JobData;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * FIXME write correct javadoc
 *
 * immutable container for job results
 *
 * a job result may contain a message, any number of table keys and corresponding schemas and at most one mllib model or
 * some other result object (some object in fact)
 *
 * @author Bjoern Lohrmann (KNIME.COM)
 */
@SparkClass
public class JobserverJobOutput extends JobData {

    private static final String JOBSERVER_PREFIX = "js";

    private static final String KEY_ERROR_THROWABLE = "throwable";

    private static final String KEY_ERROR_FLAG = "error";

    private static final String KEY_LOGMSG = "log";

    /**
     * Creates an empty instance of this class that holds the results of a successful Spark job execution.
     *
     */
    JobserverJobOutput() {
        super(JOBSERVER_PREFIX);
    }

    /**
     * Creates an instance of this class that holds the results of a successful Spark job execution and which is backed
     * by the given internal map.
     */
    JobserverJobOutput(final Map<String, Object> internalMap) {
        super(JOBSERVER_PREFIX, internalMap);
    }

    /**
     * @return whether job execution failed or not. If this method returns true, then {@link #getThrowable()} returns
     *         the Throwable that caused the failure.
     */
    public boolean isError() {
        return getOrDefault(KEY_ERROR_FLAG, false);
    }

    /**
     *
     * @return a {@link #getThrowable()} that caused the job to fail
     */
    public Throwable getThrowable() {
        return get(KEY_ERROR_THROWABLE);
    }

    /**
     * @return an (unmodifiable) list of {@link LogMessage} objects, ordered in ascending order by timestamp (i-th
     *         message has timestamp <= than (i+1)-th message).
     */
    public List<LogMessage> getLogMessages() {
        return Collections.unmodifiableList(getOrDefault(KEY_LOGMSG, new LinkedList<LogMessage>()));
    }

    /**
     * Instantiates and returns the wrapped {@link JobOutput} backed by the same internal map.
     *
     * @param outputClass the job output {@link Class}
     * @return The wrapped spark job output
     * @throws InstantiationException If something went wrong during instantiation.
     * @throws IllegalAccessException If something went wrong during instantiation.
     */
    public <T extends JobOutput> T getSparkJobOutput(final Class<T> outputClass)
        throws InstantiationException, IllegalAccessException {

        final T jobOutput = outputClass.newInstance();
        jobOutput.setInternalMap(getInternalMap());

        return jobOutput;
    }

    /**
     * Create a {@link JobserverJobOutput} indicating successful job execution with a resulting {@link JobOutput}.
     *
     * @param jobOutput output of successful job
     *
     * @return a job result with where {@link JobserverJobOutput#isError()} will be false and which contains the data
     *         from the given job output
     */
    public static JobserverJobOutput success(final JobOutput jobOutput) {
        JobserverJobOutput jsOutput = new JobserverJobOutput(jobOutput.getInternalMap());
        return jsOutput;
    }

    /**
     * Create an empty {@link JobserverJobOutput} indicating successful job.
     *
     * @return a job result with where {@link JobserverJobOutput#isError()} will be false and which contains the data
     *         from the given job output
     */
    public static JobserverJobOutput success() {
        return new JobserverJobOutput();
    }

    /**
     * Creates a {@link JobserverJobOutput} indicating a failed job execution and which contains the given
     * {@link Throwable}.
     *
     * @param throwable The exeption to report back to the client
     * @return a {@link JobserverJobOutput} indicating a failed job execution and which contains the given
     *         {@link Throwable}.
     */
    public static JobserverJobOutput failure(final Throwable throwable) {
        JobserverJobOutput toReturn = new JobserverJobOutput();
        toReturn.set(KEY_ERROR_FLAG, true);
        toReturn.set(KEY_ERROR_THROWABLE, throwable);
        return toReturn;
    }

    /**
     *
     * @param logMessages The log messages to add. You should make sure to add log messages in ascending timestamp order
     *            (i-th message has timestamp <= than (i+1)-th message).
     * @return this with added log messages
     */
    public JobserverJobOutput withLogMessages(final List<LogMessage> logMessages) {
        LinkedList<LogMessage> existingLogMessages = getOrDefault(KEY_LOGMSG, new LinkedList<LogMessage>());
        existingLogMessages.addAll(logMessages);
        set(KEY_LOGMSG, existingLogMessages);
        return this;
    }

    /**
     *
     * @param internalMap
     * @return a {@link JobserverJobOutput} backed by the given internal map.
     */
    public static JobserverJobOutput fromMap(final Map<String, Object> internalMap) {
        JobserverJobOutput jsOutput = new JobserverJobOutput(internalMap);
        return jsOutput;
    }
}
