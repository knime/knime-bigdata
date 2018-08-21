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
 *   Created on 24.06.2016 by koetter
 */
package org.knime.bigdata.commons.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.SSLFactory.Mode;
import org.eclipse.jface.preference.IPreferenceStore;
import org.knime.bigdata.commons.CommonsPlugin;
import org.knime.bigdata.commons.config.eclipse.CommonPreferenceInitializer;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeLogger.LEVEL;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.KNIMERuntimeContext;

/**
 * Container class that holds configuration information for the different Big Data Extensions.
 * @author Tobias Koetter, KNIME.com
 */
public class CommonConfigContainer {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(CommonConfigContainer.class);

    private static final CommonConfigContainer instance = new CommonConfigContainer();

    private static IPreferenceStore PREFERENCE_STORE = CommonsPlugin.getDefault().getPreferenceStore();

    private boolean m_hdfsSupported = false;

    private boolean m_hiveSupported = false;

    private boolean m_sparkSupported = false;

    /**The placeholder for the workflow user in the JDBC impersonation parameter.*/
    public static final String JDBC_IMPERSONATION_PLACEHOLDER = "{1}";

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

    /** @return <code>true</code> if trust store configuration is available */
    public boolean hasSSLConfig() {
        return hasSSLTruststoreConfig() || hasSSLKeystoreConfig();
    }

    /** @return <code>true</code> if trust store configuration is available */
    public boolean hasSSLTruststoreConfig() {
        return PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_TRUSTSTORE_ENABLE);
    }

    /** @return <code>true</code> if trust store configuration is available */
    public boolean hasSSLKeystoreConfig() {
        return PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_KEYSTORE_ENABLE);
    }

    /** @param conf - Hadoop configuration to add SSL configuration. */
    public void addSSLConfig(final Configuration conf) {
        if (PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_TRUSTSTORE_ENABLE)) {
            addSSLConfig(conf, CommonPreferenceInitializer.PREF_TRUSTSTORE_HOSTNAME_VERIFIER, SSLFactory.SSL_HOSTNAME_VERIFIER_KEY);
        }

        if (PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_KEYSTORE_ENABLE)) {
            conf.setBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, true);
        }
    }

    /** @param conf - Hadoop configuration to add SSL client configuration. */
    public void addSSLClientConfig(final Configuration conf) {
        if (PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_TRUSTSTORE_ENABLE)) {
            addSSLConfig(conf, CommonPreferenceInitializer.PREF_TRUSTSTORE_LOCATION, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY);
            addSSLConfig(conf, CommonPreferenceInitializer.PREF_TRUSTSTORE_PASSWORD, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY);
            addSSLConfig(conf, CommonPreferenceInitializer.PREF_TRUSTSTORE_TYPE, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_TYPE_TPL_KEY);
            conf.setLong(FileBasedKeyStoresFactory.resolvePropertyName(Mode.CLIENT, CommonPreferenceInitializer.PREF_TRUSTSTORE_RELOAD_INTERVAL),
                PREFERENCE_STORE.getLong(CommonPreferenceInitializer.PREF_TRUSTSTORE_RELOAD_INTERVAL));
        }

        if (PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_KEYSTORE_ENABLE)) {
            addSSLConfig(conf, CommonPreferenceInitializer.PREF_KEYSTORE_LOCATION, FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY);
            addSSLConfig(conf, CommonPreferenceInitializer.PREF_KEYSTORE_PASSWORD, FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY);
            addSSLConfig(conf, CommonPreferenceInitializer.PREF_KEYSTORE_KEYPASSWORD, FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY);
            addSSLConfig(conf, CommonPreferenceInitializer.PREF_KEYSTORE_TYPE, FileBasedKeyStoresFactory.SSL_KEYSTORE_TYPE_TPL_KEY);
        }
    }

    private void addSSLConfig(final Configuration conf, final String inputKey, final String outputKey) {
        conf.set(FileBasedKeyStoresFactory.resolvePropertyName(Mode.CLIENT, outputKey), PREFERENCE_STORE.getString(inputKey));
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
     * @return <code>true</code> if Kerberos impersonation should be enabled
     */
    public boolean enableKerberosImpersonation() {
        return PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_KERBEROS_IMPERSONATION_PARAM);
    }

    /**
     * This method returns the login of the workflow user if Kerberos impersonation is enabled, the workflow is
     * executed on the KNIME Server and a workflow user is present in the {@link NodeContext}.
     * On the KNIME Analytics Platform this method always returns an empty optional.
     *
     * @return the optional login of the workflow user
     *
     * @see #enableKerberosImpersonation()
     */
    public Optional<String> getUserToImpersonate() {
        Optional<String> wfUser = Optional.empty();
        if (KNIMERuntimeContext.INSTANCE.runningInServerContext()) {
            if (enableKerberosImpersonation()) {
                wfUser = NodeContext.getWorkflowUser();
                if (!wfUser.isPresent()) {
                    LOGGER.warn("Kerberos impersonation disabled on KNIME Server because no workflow user is present in node context.");
                }
            } else {
                LOGGER.info("Kerberos impersonation disabled on KNIME Server");
            }
        }
        return wfUser;
    }

    /**
     * @return the file path to the custom kerberos keytab or <code>null</code> if the default should be used
     */
    public String getKerberosKeytabConfig() {
        return PREFERENCE_STORE.getString(CommonPreferenceInitializer.PREF_KERBEROS_KEYTAB_FILE);
    }

    /**
     * @return <code>true</code> if a kerberos keytab file is available
     */
    public boolean hasKerberosKeytabConfig() {
        return !PREFERENCE_STORE.isDefault(CommonPreferenceInitializer.PREF_KERBEROS_KEYTAB_FILE);
    }

    /**
     * @return <code>true</code> if JDBC parameter based user impersonation should be used
     */
    public boolean useJDBCImpersonationParameter() {
        return PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_KERBEROS_JDBC_IMPERSONATION_PARAM_FLAG);
    }

    /**
     * @return the JDBC impersonation parameter whereas {1} should be replaced by the workflow user
     */
    public String getJDBCImpersonationParameter() {
        return PREFERENCE_STORE.getString(CommonPreferenceInitializer.PREF_KERBEROS_JDBC_IMPERSONATION_PARAM);
    }

    /**
     * @return true if Kerberos logging is enabled.
     */
    public boolean isKerberosLoggingEnabled() {
        return PREFERENCE_STORE.getBoolean(CommonPreferenceInitializer.PREF_KERBEROS_LOGGING_ENABLED);
    }

    /**
     * @return the Kerberos logging {@link LEVEL}
     */
    public LEVEL getKerberosLoggingLevel() {
        final String levelString = PREFERENCE_STORE.getString(CommonPreferenceInitializer.PREF_KERBEROS_LOGGING_LEVEL);
        return LEVEL.valueOf(levelString);
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
