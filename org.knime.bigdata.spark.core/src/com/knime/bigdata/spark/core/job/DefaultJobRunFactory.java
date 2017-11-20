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
 *   Created on 29.01.2016 by koetter
 */
package com.knime.bigdata.spark.core.job;

/**
 * Default implementation for a {@link JobRunFactory}.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class DefaultJobRunFactory<I extends JobInput, O extends JobOutput> implements JobRunFactory<I, O> {

    private final String m_jobId;

    private final Class<?> m_jobClass;

    private final Class<O> m_jobOutputClass;

    private final ClassLoader m_jobOutputClassLoader;


    public DefaultJobRunFactory(final String jobId, final Class<?> jobClass, final Class<O> jobOutputClass) {
        this(jobId, jobClass, jobOutputClass, jobClass.getClassLoader());
    }

    public DefaultJobRunFactory(final String jobId, final Class<?> jobClass, final Class<O> jobOutputClass,
        final ClassLoader jobOutputClassLoader) {

        m_jobId = jobId;
        m_jobClass = jobClass;
        m_jobOutputClass = jobOutputClass;
        m_jobOutputClassLoader = jobOutputClassLoader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getJobID() {
        return m_jobId;
    }

    /**
     * @return the class of the job
     */
    public Class<?> getJobClass() {
        return m_jobClass;
    }

    /**
     * @return the class of the output that the job produces
     */
    public Class<O> getJobOutputClass() {
        return m_jobOutputClass;
    }

    /**
     * @return the class loader with which to load instance of the job output class.
     */
    public ClassLoader getJobOutputClassLoader() {
        return m_jobOutputClassLoader;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public JobRun<I, O> createRun(final I input) {
        return new DefaultJobRun<I, O>(input, m_jobClass, m_jobOutputClass, m_jobOutputClassLoader);
    }
}
