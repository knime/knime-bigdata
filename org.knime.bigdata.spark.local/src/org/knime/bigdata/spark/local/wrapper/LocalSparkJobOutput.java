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
package org.knime.bigdata.spark.local.wrapper;

import java.util.Map;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobData;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Generic job output class for Spark jobs being run on local Spark.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class LocalSparkJobOutput extends JobData {

    private static final String JOBSERVER_PREFIX = "js";

    private static final String KEY_ERROR_THROWABLE = "throwable";

    private static final String KEY_ERROR_FLAG = "error";

    /**
     * Creates an empty instance of this class that holds the results of a successful Spark job execution.
     *
     */
    LocalSparkJobOutput() {
        super(JOBSERVER_PREFIX);
    }

    /**
     * Creates an instance of this class that holds the results of a successful Spark job execution and which is backed
     * by the given internal map.
     */
    LocalSparkJobOutput(final Map<String, Object> internalMap) {
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
     * @return the {@link KNIMESparkException} that caused the job to fail
     */
    public KNIMESparkException getException() {
        return get(KEY_ERROR_THROWABLE);
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
     * Create a {@link LocalSparkJobOutput} indicating successful job execution with a resulting {@link JobOutput}.
     *
     * @param jobOutput output of successful job
     *
     * @return a job result with where {@link LocalSparkJobOutput#isError()} will be false and which contains the data
     *         from the given job output
     */
    public static LocalSparkJobOutput success(final JobOutput jobOutput) {
        LocalSparkJobOutput jsOutput = new LocalSparkJobOutput(jobOutput.getInternalMap());
        return jsOutput;
    }

    /**
     * Create an empty {@link LocalSparkJobOutput} indicating successful job.
     *
     * @return a job result with where {@link LocalSparkJobOutput#isError()} will be false and which contains the data
     *         from the given job output
     */
    public static LocalSparkJobOutput success() {
        return new LocalSparkJobOutput();
    }

    /**
     * Creates a {@link LocalSparkJobOutput} indicating a failed job execution and which contains the given
     * {@link Throwable}.
     *
     * @param exception The exception to report back to the client
     * @return a {@link LocalSparkJobOutput} indicating a failed job execution and which contains the given
     *         {@link Throwable}.
     */
    public static LocalSparkJobOutput failure(final KNIMESparkException exception) {
        LocalSparkJobOutput toReturn = new LocalSparkJobOutput();
        toReturn.set(KEY_ERROR_FLAG, true);
        toReturn.set(KEY_ERROR_THROWABLE, exception);
        return toReturn;
    }

    /**
     *
     * @param internalMap
     * @return a {@link LocalSparkJobOutput} backed by the given internal map.
     */
    public static LocalSparkJobOutput fromMap(final Map<String, Object> internalMap) {
        LocalSparkJobOutput jsOutput = new LocalSparkJobOutput(internalMap);
        return jsOutput;
    }
}
