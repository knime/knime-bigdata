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
 *   Created on Apr 12, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.job;

import java.io.File;
import java.util.List;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 * @param <I>
 * @param <O>
 */
public class DefaultJobWithFilesRun<I extends JobInput, O extends JobOutput> extends DefaultJobRun<I, O>
    implements JobWithFilesRun<I, O> {

    private final List<File> m_inputFiles;

    private final FileLifetime m_filesLifetime;

    private final boolean m_useInputFileCopyCache;

    /**
     * @param input {@link JobInput}
     * @param sparkJobClass Spark job class
     * @param jobOutputClass the {@link JobOutput} class
     * @param inputFiles the input file
     * @param filesLifetime the {@link JobWithFilesRun.FileLifetime}
     */
    public DefaultJobWithFilesRun(final I input, final Class<?> sparkJobClass, final Class<O> jobOutputClass, final List<File> inputFiles,
        final FileLifetime filesLifetime) {
        this(input, sparkJobClass, jobOutputClass, sparkJobClass.getClassLoader(), inputFiles, filesLifetime, false);
    }

    /**
     * @param input {@link JobInput}
     * @param sparkJobClass Spark job class
     * @param jobOutputClass the {@link JobOutput} class
     * @param jobOutputClassLoader the {@link JobOutput} {@link ClassLoader}
     * @param inputFiles the input file
     * @param filesLifetime the {@link JobWithFilesRun.FileLifetime}
     * @param useInputFileCopyCache <code>true</code> if the copy cache should be used for the input files
     */
    public DefaultJobWithFilesRun(final I input, final Class<?> sparkJobClass, final Class<O> jobOutputClass,
        final ClassLoader jobOutputClassLoader, final List<File> inputFiles,
        final FileLifetime filesLifetime, final boolean useInputFileCopyCache) {
        super(input, sparkJobClass, jobOutputClass, jobOutputClassLoader);
        m_inputFiles = inputFiles;
        m_filesLifetime = filesLifetime;
        m_useInputFileCopyCache = useInputFileCopyCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<File> getInputFiles() {
        return m_inputFiles;
    }

    @SuppressWarnings("unchecked")
    @Override
    public O run(final SparkContextID contextID, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {
        return (O) SparkContextManager.getOrCreateSparkContext(contextID).startJobAndWaitForResult(this, exec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileLifetime getInputFilesLifetime() {
        return m_filesLifetime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useInputFileCopyCache() {
        return m_useInputFileCopyCache;
    }
}
