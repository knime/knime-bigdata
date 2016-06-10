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
 *   Created on Apr 13, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context;

import com.knime.bigdata.spark.core.exception.MissingJobException;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.job.JobRunFactoryRegistry;
import com.knime.bigdata.spark.core.job.JobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkContextUtil {

    /**
     * Finds and returns a {@link JobRunFactory} for the given job id and which matches the Spark version of the given
     * context.
     *
     * @param contextID
     * @param jobId
     * @return a matching {@link JobRunFactory}
     * @throws MissingJobException If no matching factory exists.
     * @throws ClassCastException If factories of the given job id are not derived from {@link JobRunFactory}
     */
    public static <I extends JobInput, O extends JobOutput> JobRunFactory<I, O>
        getJobRunFactory(final SparkContextID contextID, final String jobId) throws MissingJobException {

        SparkVersion contextSparkVersion = SparkContextManager.getOrCreateSparkContext(contextID).getSparkVersion();

        return JobRunFactoryRegistry.getFactory(jobId, contextSparkVersion);
    }

    /**
     * Finds and returns a {@link JobWithFilesRunFactory} for the given job id and which matches the Spark version of
     * the given context.
     *
     * @param contextID
     * @param jobId
     * @return a matching {@link JobWithFilesRunFactory}
     * @throws MissingJobException If no matching factory exists.
     * @throws ClassCastException If factories of the given job id are not derived from {@link JobWithFilesRunFactory}
     */
    public static <I extends JobInput, O extends JobOutput> JobWithFilesRunFactory<I, O>
        getJobWithFilesRunFactory(final SparkContextID contextID, final String jobId) throws MissingJobException {

        SparkVersion contextSparkVersion = SparkContextManager.getOrCreateSparkContext(contextID).getSparkVersion();

        return (JobWithFilesRunFactory<I, O>)JobRunFactoryRegistry.<I, O> getFactory(jobId, contextSparkVersion);
    }

    /**
     * Finds and returns a {@link SimpleJobRunFactory} for the given job id and which matches the Spark version of the
     * given context.
     *
     * @param contextID
     * @param jobId
     * @return a matching {@link SimpleJobRunFactory}
     * @throws MissingJobException If no matching factory exists.
     * @throws ClassCastException If factories of the given job id are not derived from {@link SimpleJobRunFactory}
     */
    public static <I extends JobInput> SimpleJobRunFactory<I> getSimpleRunFactory(final SparkContextID contextID,
        final String jobId) throws MissingJobException {

        SparkVersion contextSparkVersion = SparkContextManager.getOrCreateSparkContext(contextID).getSparkVersion();

        return (SimpleJobRunFactory<I>)JobRunFactoryRegistry.<I, EmptyJobOutput> getFactory(jobId, contextSparkVersion);
    }
}
