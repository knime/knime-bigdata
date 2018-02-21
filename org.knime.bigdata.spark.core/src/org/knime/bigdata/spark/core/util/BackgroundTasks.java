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
 *   Created on Feb 21, 2018 by bjoern
 */
package org.knime.bigdata.spark.core.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This class provides methods for background (i.e. non-blocking) execution of tasks. Currently, this executes the tasks
 * on a single-threaded execuctor service, however this may change.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class BackgroundTasks {

    private final static ExecutorService BACKGROUND_EXECUTOR = Executors.newSingleThreadExecutor();

    /**
     * Runs the given task in the background (i.e. non-blocking).
     *
     * @param toRun The task to run in the background.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException if the task is null
     * @see ExecutorService#submit(Runnable)
     */
    public static Future<?> run(final Runnable toRun) {
        return BACKGROUND_EXECUTOR.submit(toRun);
    }

    /**
     * Runs the given value-returning task in the background (i.e. non-blocking).
     *
     * @param toRun The task to run in the background.
     * @param <T> the type of the result.
     * @return a Future representing pending completion of the task, that can be used to get the value return by the
     *         task.
     * @throws RejectedExecutionException if the task cannot be scheduled for execution.
     * @throws NullPointerException if the task is null.
     * @see ExecutorService#submit(Callable)
     */
    public static <T> Future<T> run(final Callable<T> toRun) {
        return BACKGROUND_EXECUTOR.submit(toRun);
    }

    /**
     * Method to be called to shutdown the underlying executor service. This method should only ever be called when the
     * Spark extension shuts down (which only happens when KNIME shuts down).
     *
     * @throws InterruptedException If interrupted while waiting.
     */
    public static void shutdown() throws InterruptedException {
        BACKGROUND_EXECUTOR.shutdown();
        BACKGROUND_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS);
    }
}
