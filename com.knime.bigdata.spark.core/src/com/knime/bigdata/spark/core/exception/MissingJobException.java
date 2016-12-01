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
 *   Created on 29.01.2016 by koetter
 */
package com.knime.bigdata.spark.core.exception;

import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MissingJobException extends KNIMESparkException {

    private static final long serialVersionUID = 1L;
    private final SparkVersion m_sparkVersion;
    private final String m_jobId;

    /**
     * Constructor.
     * @param jobId the unique job id
     * @param sparkVersion the {@link SparkVersion} the job isn't available for
     */
    public MissingJobException(final String jobId, final SparkVersion sparkVersion) {
        super("Could not find required Spark job for Spark version: " + sparkVersion.getLabel()
            +". Possible reason: The extension that provides the KNIME Spark Executor jobs for your Spark version is not installed."
            + " Job id: " + jobId);
        m_jobId = jobId;
        m_sparkVersion = sparkVersion;
    }

    /**
     * @return the {@link SparkVersion} the job couldn't be found for
     */
    public SparkVersion getSparkVersion() {
        return m_sparkVersion;
    }

    /**
     * @return the jobId
     */
    public String getJobId() {
        return m_jobId;
    }
}
