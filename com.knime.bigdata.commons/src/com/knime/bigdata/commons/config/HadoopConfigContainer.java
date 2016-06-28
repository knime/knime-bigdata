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
 *   Created on 24.06.2016 by koetter
 */
package com.knime.bigdata.commons.config;

import java.io.InputStream;

/**
 * Container class that holds configuration information for the different Big Data Extensions.
 * @author Tobias Koetter, KNIME.com
 */
public class HadoopConfigContainer {

    private static final HadoopConfigContainer instance = new HadoopConfigContainer();

    private boolean m_hdfsSupported = false;

    private boolean m_hiveSupported = false;

    private boolean m_sparkSupported = false;

    private HadoopConfigContainer() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
    */
    public static HadoopConfigContainer getInstance() {
        return instance;
    }

    /**
     * @return an {@link InputStream} with the hadoop-site.xml or <code>null</code> if the default should be used
     */
    public InputStream getHadoopConfig() {
        //TODO: Get the file from the preference page
        return null;
    }

    /**
     * @return <code>true</code> if a hadoop-site.xml is available
     */
    public boolean hasHadoopConfig() {
        return getHadoopConfig() != null;
    }

    /**
     * @return the hdfsSupported
     */
    public boolean isHdfsSupported() {
        return m_hdfsSupported;
    }

    /**
     * @return the hiveSupported
     */
    public boolean isHiveSupported() {
        return m_hiveSupported;
    }

    /**
     * @return the sparkSupported
     */
    public boolean isSparkSupported() {
        return m_sparkSupported;
    }

    /**
     * @noreference This method should be used by other plugins then the hdfs file handling plugin
     */
    public void hdfsSupported() {
        m_hdfsSupported = true;
    }

    /**
     * @noreference This method should be used by other plugins then the hdfs file handling plugin
     */
    public void hiveSupported() {
        m_hiveSupported = true;
    }

    /**
     * @noreference This method should be used by other plugins then the hdfs file handling plugin
     */
    public void sparkSupported() {
        m_sparkSupported = true;
    }
}
