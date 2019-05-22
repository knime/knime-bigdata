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
 *   Created on May 3, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.core.job;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;

/**
 * Class that job server implementations like Livy, local or Spark Job Server should extend to return job server
 * specific results. Use {@link JobOutput} for normal job outputs instead!
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class WrapperJobOutput extends JobData {

    private static final String PREFIX = "js";

    private static final String KEY_ERROR_THROWABLE = "throwable";

    private static final String KEY_ERROR_FLAG = "error";

    private static final String KEY_STAT = "statistics";

    /**
     * Creates an empty instance of this class.
     */
    public WrapperJobOutput() {
        super(PREFIX);
    }

    /**
     * Creates an instance of this class that holds the results of a successful Spark job execution and which is backed
     * by the given internal map.
     *
     * @param internalMap map with job result values
     */
    private WrapperJobOutput(final Map<String, Object> internalMap, final List<Path> files) {
        super(PREFIX, internalMap);
        for (Path file : files) {
            withFile(file);
        }
    }

    /**
     * Creates an instance indicating a job execution and which contains the given {@link KNIMESparkException}.
     *
     * @param KNIMESparkException The exeption to report back to the client
     */
    private WrapperJobOutput(final KNIMESparkException throwable) {
        super(PREFIX);
        set(KEY_ERROR_FLAG, true);
        set(KEY_ERROR_THROWABLE, throwable);
    }

    /**
     * @return whether job execution failed or not. If this method returns true, then {@link #getException()} returns
     *         the Throwable that caused the failure.
     */
    public boolean isError() {
        return getOrDefault(KEY_ERROR_FLAG, false);
    }

    /**
     * @return a {@link Throwable} that caused the job to fail
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
        for (Path file : getFiles()) {
            jobOutput.withFile(file);
        }

        return jobOutput;
    }

    /**
     * Sets the {@link NamedObjectStatistics} of a named object.
     *
     * @param name name of the named object
     * @param stat statistic of the named object
     */
    public void setNamedObjectStatistic(final String name, final NamedObjectStatistics stat) {
        Map<String, NamedObjectStatistics> stats = get(KEY_STAT);
        if (stats == null) {
            stats = new HashMap<>();
            set(KEY_STAT, stats);
        }
        stats.put(name, stat);
    }

    /**
     * @return Map of named object names and named object statistics
     */
    public Map<String, NamedObjectStatistics> getNamedObjectStatistics() {
        return get(KEY_STAT);
    }

    /**
     * Create a {@link WrapperJobOutput} indicating successful job execution with a resulting {@link JobOutput}.
     *
     * @param jobOutput output of successful job
     *
     * @return a job result with where {@link WrapperJobOutput#isError()} will be false and which contains the data from
     *         the given job output
     */
    public static WrapperJobOutput success(final JobOutput jobOutput) {
        return new WrapperJobOutput(jobOutput.getInternalMap(), jobOutput.getFiles());
    }

    /**
     * Create an empty {@link WrapperJobOutput} indicating successful job.
     *
     * @return a job result with where {@link WrapperJobOutput#isError()} will be false and which contains the data from
     *         the given job output
     */
    public static WrapperJobOutput success() {
        return new WrapperJobOutput();
    }

    /**
     * Create an empty {@link WrapperJobOutput}.
     *
     * @return an empty job result
     */
    public static WrapperJobOutput empty() {
        return new WrapperJobOutput();
    }

    /**
     * Creates a {@link WrapperJobOutput} indicating a failed job execution and which contains the given {@link Throwable}.
     *
     * @param throwable The exception to report back to the client
     * @return a {@link WrapperJobOutput} indicating a failed job execution and which contains the given {@link Throwable}.
     */
    public static WrapperJobOutput failure(final KNIMESparkException throwable) {
        return new WrapperJobOutput(throwable);
    }
}
