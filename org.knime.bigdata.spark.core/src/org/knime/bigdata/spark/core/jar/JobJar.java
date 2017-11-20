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
 *   Created on Apr 27, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.jar;

import java.io.File;

import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Bjoern Lohrmann
 */
public class JobJar {

    private final File m_jarFile;

    private final JobJarDescriptor m_descriptor;

    /**
     * @param jobJarFile
     * @param descr
     */
    public JobJar(final File jobJarFile, final JobJarDescriptor descr) {
        m_jarFile = jobJarFile;
        m_descriptor = descr;
    }

    /**
     * @return the Spark version the job jar was created for.
     */
    public SparkVersion getSparkVersion() {
        return SparkVersion.fromString(m_descriptor.getSparkVersion());
    }

    /**
     * @return a local file system location where the job jar is stored.
     */
    public File getJarFile() {
        return m_jarFile;
    }

    /**
     * @return the info
     */
    public JobJarDescriptor getDescriptor() {
        return m_descriptor;
    }
}
