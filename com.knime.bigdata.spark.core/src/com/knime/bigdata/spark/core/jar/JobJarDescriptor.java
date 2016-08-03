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
 *   Created on 22.03.2016 by koetter
 */
package com.knime.bigdata.spark.core.jar;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class JobJarDescriptor {

    private static final String KEY_JOB_JAR_HASH = "jobJarHash";

    private static final String KEY_PLUGIN_VERSION = "pluginVersion";

    private static final String KEY_SPARK_VERSION = "sparkVersion";

    private static final String KEY_JOBSERVER_JOB_CLASS = "jobserverJobClass";

    private static final String KEY_PROVIDER_IDS = "providerIDs";

    /** The file name of the job jar info object. */
    public static final String FILE_NAME = "KNIMEJobJarDescriptor.properties";

    private final String m_hash;

    private final String m_pluginVersion;

    private final String m_sparkVersion;

    private final String m_jobserverJobClass;

    /**
     * A set of provider IDs that have contributed
     *
     * @since 1.6.0.20160803 (i.e. added as part of issue BD-175 *after* the July 2016 release)
     */
    private final Set<String> m_providerIDs;

    /**
     * Creates a new job jar descriptor.
     *
     * @param pluginVersion
     * @param sparkVersion
     * @param hash
     * @param jobserverJobClass
     * @param providerIDs
     */
    public JobJarDescriptor(final String pluginVersion, final String sparkVersion, final String hash,
        final String jobserverJobClass, final Set<String> providerIDs) {
        m_pluginVersion = pluginVersion;
        m_sparkVersion = sparkVersion;
        m_hash = hash;
        m_jobserverJobClass = jobserverJobClass;
        m_providerIDs = providerIDs;
    }

    /**
     * @return a hash that is computed over the contents of the jar file (or other information that uniquely identifies
     *         the contents of the file).
     */
    public String getHash() {
        return m_hash;
    }

    /**
     * @return the pluginVersion
     */
    public String getPluginVersion() {
        return m_pluginVersion;
    }

    /**
     * @return the sparkVersion
     */
    public String getSparkVersion() {
        return m_sparkVersion;
    }

    /**
     *
     * @return the IDs (including version strings) of plugins that have contributed classes to the jar file
     */
    public Set<String> getProviderIDs() {
        return m_providerIDs;
    }

    /**
     * @return the Spark Jobserver job class, or null, if no class has been registered.
     */
    public String getJobserverJobClass() {
        return m_jobserverJobClass;
    }

    /**
     * @param is {@link InputStream} to read from
     * @return the {@link JobJarDescriptor} object with the information from the given input stream
     * @throws IOException
     */
    public static JobJarDescriptor load(final InputStream is) throws IOException {
        Properties prop = new Properties();
        prop.load(is);

        return new JobJarDescriptor(prop.getProperty(KEY_PLUGIN_VERSION), prop.getProperty(KEY_SPARK_VERSION),
            prop.getProperty(KEY_JOB_JAR_HASH), prop.getProperty(KEY_JOBSERVER_JOB_CLASS),
            parseProviderIDs(prop.getProperty(KEY_PROVIDER_IDS)));
    }

    private static Set<String> parseProviderIDs(final String providerIds) {
        final Set<String> providerIDs = new HashSet<String>();

        if (providerIds != null) {
            providerIDs.addAll(Arrays.asList(providerIds.split(",")));
        }

        return providerIDs;
    }

    private String serializeProviderIDs(final Set<String> providerIDs) {
        final StringBuilder buf = new StringBuilder();

        for(String providerID : providerIDs) {
            buf.append(providerID);
            buf.append(",");
        }

        if (!providerIDs.isEmpty()) {
            buf.deleteCharAt(buf.length() -1);
        }

        return buf.toString();
    }

    /**
     * @param os {@link OutputStream} to write to
     * @throws IOException
     */
    public void save(final OutputStream os) throws IOException {
        final Properties prop = new Properties();
        prop.setProperty(KEY_PLUGIN_VERSION, getPluginVersion());
        prop.setProperty(KEY_SPARK_VERSION, getSparkVersion());
        prop.setProperty(KEY_JOB_JAR_HASH, getHash());
        if (getJobserverJobClass() != null) {
            prop.setProperty(KEY_JOBSERVER_JOB_CLASS, getJobserverJobClass());
        }
        prop.setProperty(KEY_PROVIDER_IDS, serializeProviderIDs(getProviderIDs()));
        prop.store(os, "KNIME Job jar information");
    }
}
