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
package org.knime.bigdata.spark.core.livy.jobapi;

import java.util.Map;

import org.knime.bigdata.spark.core.job.JobData;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * A class that wraps the actual job output or exception of Spark jobs on Livy.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class LivyJobOutput extends JobData {

    private static final String LIVY_PREFIX = "ly";

    private static final String KEY_ERROR_THROWABLE = "throwable";

    private static final String KEY_ERROR_FLAG = "error";

    /**
     * Creates an empty instance of this class that holds the results of a successful Spark job execution.
     *
     */
    LivyJobOutput() {
        super(LIVY_PREFIX);
    }

    /**
     * Creates an instance of this class that holds the results of a successful Spark job execution and which is backed
     * by the given internal map.
     */
    LivyJobOutput(final Map<String, Object> internalMap) {
        super(LIVY_PREFIX, internalMap);
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
     * Create a {@link LivyJobOutput} indicating successful job execution with a resulting {@link JobOutput}.
     *
     * @param jobOutput output of successful job
     *
     * @return a job result with where {@link LivyJobOutput#isError()} will be false and which contains the data from
     *         the given job output
     */
    public static LivyJobOutput success(final JobOutput jobOutput) {
        return new LivyJobOutput(jobOutput.getInternalMap());
    }

    /**
     * Create an empty {@link LivyJobOutput} indicating successful job.
     *
     * @return a job result with where {@link LivyJobOutput#isError()} will be false and which contains the data from
     *         the given job output
     */
    public static LivyJobOutput success() {
        return new LivyJobOutput();
    }

    /**
     * Creates a {@link LivyJobOutput} indicating a failed job execution and which contains the given {@link Throwable}.
     *
     * @param throwable The exeption to report back to the client
     * @return a {@link LivyJobOutput} indicating a failed job execution and which contains the given {@link Throwable}.
     */
    public static LivyJobOutput failure(final Throwable throwable) {
        LivyJobOutput toReturn = new LivyJobOutput();
        toReturn.set(KEY_ERROR_FLAG, true);
        toReturn.set(KEY_ERROR_THROWABLE, throwable);
        return toReturn;
    }

    /**
     * Creates a new {@link LivyJobOutput} backed by the given map.
     * 
     * @param internalMap
     * @return a {@link LivyJobOutput} backed by the given internal map.
     */
    public static LivyJobOutput fromMap(final Map<String, Object> internalMap) {
        return new LivyJobOutput(internalMap);
    }
}
