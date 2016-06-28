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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.eclipse.jface.preference.IPreferenceStore;

import com.knime.bigdata.commons.CommonsPlugin;
import com.knime.bigdata.commons.config.eclipse.CommonPreferenceInitializer;

/**
 * Container class that holds configuration information for the different Big Data Extensions.
 * @author Tobias Koetter, KNIME.com
 */
public class CommonConfigContainer {

    private static final CommonConfigContainer instance = new CommonConfigContainer();

    private static IPreferenceStore PREFERENCE_STORE = CommonsPlugin.getDefault().getPreferenceStore();

    private boolean m_hdfsSupported = false;

    private boolean m_hiveSupported = false;

    private boolean m_sparkSupported = false;

    private CommonConfigContainer() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
    */
    public static CommonConfigContainer getInstance() {
        return instance;
    }

    /**
     * @return an {@link InputStream} with the core-site.xml or <code>null</code> if the default should be used
     */
    public InputStream getCoreSiteConfig() {
        try {
            return new FileInputStream(PREFERENCE_STORE.getString(CommonPreferenceInitializer.PREF_CORE_SITE_FILE));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Can't find custom core-site.xml file: " + e);
        }
    }

    /**
     * @return <code>true</code> if a core-site.xml is available
     */
    public boolean hasCoreSiteConfig() {
        return !PREFERENCE_STORE.isDefault(CommonPreferenceInitializer.PREF_CORE_SITE_FILE);
    }

    /**
     * @return an {@link InputStream} with the hdfs-site.xml or <code>null</code> if the default should be used
     */
    public InputStream getHdfsSiteConfig() {
        try {
            return new FileInputStream(PREFERENCE_STORE.getString(CommonPreferenceInitializer.PREF_HDFS_SITE_FILE));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Can't find custom hdfs-site.xml file: " + e);
        }
    }

    /**
     * @return <code>true</code> if a hdfs-site.xml is available
     */
    public boolean hasHdfsSiteConfig() {
        return !PREFERENCE_STORE.isDefault(CommonPreferenceInitializer.PREF_HDFS_SITE_FILE);
    }

    /**
     * @return custom kerberos user as String or <code>null</code> if the default should be used
     */
    public String getKerberosUserConfig() {
        return PREFERENCE_STORE.getString(CommonPreferenceInitializer.PREF_KERBEROS_USER);
    }

    /**
     * @return <code>true</code> if a kerberos keytab file is available
     */
    public boolean hasKerberosUserConfig() {
        return !PREFERENCE_STORE.isDefault(CommonPreferenceInitializer.PREF_KERBEROS_USER);
    }

    /**
     * @return an {@link InputStream} with the custom kerberos keytab or <code>null</code> if the default should be used
     */
    public InputStream getKerberosKeytabConfig() {
        try {
            return new FileInputStream(PREFERENCE_STORE.getString(CommonPreferenceInitializer.PREF_KERBEROS_KEYTAB_FILE));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Can't find custom kerberos keytab file: " + e);
        }
    }

    /**
     * @return <code>true</code> if a kerberos keytab file is available
     */
    public boolean hasKerberosKeytabConfig() {
        return !PREFERENCE_STORE.isDefault(CommonPreferenceInitializer.PREF_KERBEROS_KEYTAB_FILE);
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
