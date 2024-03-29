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
 *   Created on May 3, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.job;

/**
 * {@link JobRunFactory} implementation with no output.
 * @author Bjoern Lohrmann, KNIME.com
 * @param <I> {@link JobInput}
 */
public abstract class DefaultSimpleJobRunFactory<I extends JobInput> extends DefaultJobRunFactory<I, EmptyJobOutput> implements SimpleJobRunFactory<I> {

    /**
     * @param jobId unique job id
     * @param jobClass job class name
     */
    public DefaultSimpleJobRunFactory(final String jobId, final Class<?> jobClass) {
        super(jobId, jobClass, EmptyJobOutput.class, jobClass.getClassLoader());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleJobRun<I> createRun(final I input) {
        return new DefaultSimpleJobRun<>(input, getJobClass());
    }
}
