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
 *   Created on 05.07.2015 by koetter
 */
package org.knime.bigdata.spark.core.job;

import org.knime.bigdata.spark.core.context.SparkContextConstants;
import org.knime.bigdata.spark.core.exception.MissingJobException;
import org.knime.bigdata.spark.core.version.DefaultSparkProviderRegistry;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class JobRunFactoryRegistry
    extends DefaultSparkProviderRegistry<String, JobRunFactory<?, ?>, JobRunFactoryProvider> {

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "org.knime.bigdata.spark.core.JobRunFactoryProvider";

    private static volatile JobRunFactoryRegistry instance;

    private JobRunFactoryRegistry() {
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public synchronized static JobRunFactoryRegistry getInstance() {
        if (instance == null) {
            instance = new JobRunFactoryRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getElementID(final JobRunFactory<?, ?> e) {
        return e.getJobID();
    }

    /**
     * @param jobId the unique job id
     * @param sparkVersion Spark version
     * @return the corresponding {@link JobRunFactory} or <code>null</code> if none exists
     * @throws MissingJobException if no compatible job could be found
     */
    @SuppressWarnings("unchecked")
    public static <I extends JobInput, O extends JobOutput> JobRunFactory<I, O> getFactory(final String jobId,
        final SparkVersion sparkVersion) throws MissingJobException {

        final JobRunFactory<?, ?> factory = getInstance().get(jobId, sparkVersion);
        if (factory == null) {
            throw new MissingJobException(jobId, sparkVersion);
        } else {
            return (JobRunFactory<I, O>)factory;
        }
    }

    /**
     * Tests whether any jobs are registered for the given Spark version. Since BD-1111 (Split KNIME Extension for
     * Apache Spark into legacy and current) it is possible to not have jobs installed for a particular Spark version.
     *
     * @param sparkVersion
     * @return true, if jobs are available, false otherwise.
     */
    public static boolean hasJobsForSparkVersion(final SparkVersion sparkVersion) {
        return getInstance().get(SparkContextConstants.FETCH_ROWS_JOB_ID, sparkVersion) != null;
    }
}
