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
import java.util.Properties;

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

    /**The file name of the job jar info object.*/
    public static final String FILE_NAME = "KNIMEJobJarDescriptor.properties";


    private String m_hash;

    private String m_pluginVersion;

    private String m_sparkVersion;

    private String m_jobserverJobClass;

    public JobJarDescriptor(final String pluginVersion, final String sparkVersion, final String hash, final String jobserverJobClass) {
        m_pluginVersion = pluginVersion;
        m_sparkVersion = sparkVersion;
        m_hash = hash;
        m_jobserverJobClass = jobserverJobClass;
    }

    /**
     * @return the hash
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
            prop.getProperty(KEY_JOB_JAR_HASH), prop.getProperty(KEY_JOBSERVER_JOB_CLASS));
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
        prop.store(os, "KNIME Job jar information");
    }
}
