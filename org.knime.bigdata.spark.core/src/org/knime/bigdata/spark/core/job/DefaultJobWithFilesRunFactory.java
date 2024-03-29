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
 *   Created on Apr 15, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.job;

import java.io.File;
import java.util.List;

import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;

/**
 * Default implementation of a {@link JobWithFilesRunFactory} for jobs that require input files to be uploaded.
 *
 * @author Bjoern Lohrmann, KNIME.com
 * @param <I> {@link JobInput}
 * @param <O> {@link JobOutput}
 */
public class DefaultJobWithFilesRunFactory<I extends JobInput, O extends JobOutput> extends DefaultJobRunFactory<I, O>
    implements JobWithFilesRunFactory<I, O> {

    private final FileLifetime m_filesLifetime;

    private final boolean m_useInputFileCopyCache;

    /**
     * @param jobId the unique job id
     * @param jobClass the job class name
     * @param jobOutputClass the {@link JobOutput} class
     * @param filesLifetime the {@link JobWithFilesRun.FileLifetime}
     * @param useInputFileCopyCache <code>true</code> if the copy cache should be used for the input files
     */
    public DefaultJobWithFilesRunFactory(final String jobId, final Class<?> jobClass, final Class<O> jobOutputClass,
        final FileLifetime filesLifetime, final boolean useInputFileCopyCache) {

        this(jobId, jobClass, jobOutputClass, jobClass.getClassLoader(), filesLifetime, useInputFileCopyCache);
    }

    /**
     * @param jobId the unique job id
     * @param jobClass the job class name
     * @param jobOutputClass the {@link JobOutput} class
     * @param jobOutputClassLoader the {@link JobOutput} {@link ClassLoader}
     * @param filesLifetime the {@link JobWithFilesRun.FileLifetime}
     * @param useInputFileCopyCache <code>true</code> if the copy cache should be used for the input files
     */
    public DefaultJobWithFilesRunFactory(final String jobId, final Class<?> jobClass, final Class<O> jobOutputClass,
        final ClassLoader jobOutputClassLoader, final FileLifetime filesLifetime, final boolean useInputFileCopyCache) {

        super(jobId, jobClass, jobOutputClass, jobOutputClassLoader);
        m_filesLifetime = filesLifetime;
        m_useInputFileCopyCache = useInputFileCopyCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileLifetime getFilesLifetime() {
        return m_filesLifetime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useInputFileCopyCache() {
        return m_useInputFileCopyCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRun<I, O> createRun(final I input) {
        throw new UnsupportedOperationException("Job run requires input files");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobWithFilesRun<I, O> createRun(final I input, final List<File> localFiles) {
        return new DefaultJobWithFilesRun<>(input, getJobClass(), getJobOutputClass(), getJobOutputClassLoader(),
            localFiles, m_filesLifetime, m_useInputFileCopyCache);
    }
}
