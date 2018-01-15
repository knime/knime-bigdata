/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 */
package org.knime.bigdata.commons.config.eclipse;

import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.jface.preference.IPreferenceStore;
import org.knime.bigdata.commons.CommonsPlugin;
import org.knime.core.node.NodeLogger.LEVEL;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class CommonPreferenceInitializer extends AbstractPreferenceInitializer {

    /** Preference key for the core-site.xml. */
    public static final String PREF_CORE_SITE_FILE = "org.knime.bigdata.config.core-site.file";
    /** Preference key for the hdfs-site.xml. */
    public static final String PREF_HDFS_SITE_FILE = "org.knime.bigdata.config.hdfs-site.file";

    /** Enable SSL trust store configuration (verify server). */
    public static final String PREF_TRUSTSTORE_ENABLE = "org.knime.bigdata.config.truststore.enable";
    /** Preference key for hostname verifier */
    public static final String PREF_TRUSTSTORE_HOSTNAME_VERIFIER = "org.knime.bigdata.config.truststore.hostname-verifier";
    /** Preference key for truststore location */
    public static final String PREF_TRUSTSTORE_LOCATION = "org.knime.bigdata.config.truststore.location";
    /** Preference key for truststore password */
    public static final String PREF_TRUSTSTORE_PASSWORD = "org.knime.bigdata.config.truststore.password";
    /** Preference key for truststore type */
    public static final String PREF_TRUSTSTORE_TYPE = "org.knime.bigdata.config.truststore.type";
    /** Preference key for truststore reload interval */
    public static final String PREF_TRUSTSTORE_RELOAD_INTERVAL = "org.knime.bigdata.config.truststore.reload-interval";

    /** Enable client SSL certificates (verify client). */
    public static final String PREF_KEYSTORE_ENABLE = "org.knime.bigdata.config.keystore.enable";
    /** Preference key for keystore location */
    public static final String PREF_KEYSTORE_LOCATION = "org.knime.bigdata.config.keystore.location";
    /** Preference key for keystore password */
    public static final String PREF_KEYSTORE_PASSWORD = "org.knime.bigdata.config.keystore.password";
    /** Preference key for keystore key password */
    public static final String PREF_KEYSTORE_KEYPASSWORD = "org.knime.bigdata.config.keystore.keypassword";
    /** Preference key for keystore type */
    public static final String PREF_KEYSTORE_TYPE = "org.knime.bigdata.config.keystore.type";
    /** Preference key for custom kerberos user. */
    public static final String PREF_KERBEROS_USER = "org.knime.bigdata.config.kerberos.user";
    /** Preference key for custom kerberos keytab file. */
    public static final String PREF_KERBEROS_KEYTAB_FILE = "org.knime.bigdata.config.kerberos.keytab.file";

    /**Kerberos logging flag.*/
    public static final String PREF_KERBEROS_LOGGING_ENABLED = "org.knime.bigdata.config.kerberos.logging.enabled";
    /**Kerberos logging level.*/
    public static final String PREF_KERBEROS_LOGGING_LEVEL = "org.knime.bigdata.config.kerberos.logging.level";

    /** Preference key for JDBC impersonation parameter flag. */
    public static final String PREF_KERBEROS_JDBC_IMPERSONATION_PARAM_FLAG =
            "org.knime.bigdata.config.kerberos.jdbc.impersonation.flag";
    /** Preference key for JDBC impersonation parameter. */
    public static final String PREF_KERBEROS_JDBC_IMPERSONATION_PARAM =
            "org.knime.bigdata.config.kerberos.jdbc.impersonation.param";
    /**The place holder for the workflow user in the JDBC impersonation parameter.*/
    public static final String JDBC_IMPERSONATION_PLACEHOLDER = "{1}";

    @Override
    public void initializeDefaultPreferences() {
        final IPreferenceStore store = CommonsPlugin.getDefault().getPreferenceStore();
        loadDefaultValues(store);
    }

    private void loadDefaultValues(final IPreferenceStore store) {
        store.setDefault(PREF_CORE_SITE_FILE, "");
        store.setDefault(PREF_HDFS_SITE_FILE, "");

        store.setDefault(PREF_TRUSTSTORE_ENABLE, false);
        store.setDefault(PREF_TRUSTSTORE_HOSTNAME_VERIFIER, "DEFAULT");
        store.setDefault(PREF_TRUSTSTORE_LOCATION, "");
        store.setDefault(PREF_TRUSTSTORE_PASSWORD, "");
        store.setDefault(PREF_TRUSTSTORE_TYPE, FileBasedKeyStoresFactory.DEFAULT_KEYSTORE_TYPE);
        store.setDefault(PREF_TRUSTSTORE_RELOAD_INTERVAL, FileBasedKeyStoresFactory.DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL);

        store.setDefault(PREF_KEYSTORE_ENABLE, false);
        store.setDefault(PREF_KEYSTORE_LOCATION, "");
        store.setDefault(PREF_KEYSTORE_PASSWORD, "");
        store.setDefault(PREF_KEYSTORE_KEYPASSWORD, "");
        store.setDefault(PREF_KEYSTORE_TYPE, FileBasedKeyStoresFactory.DEFAULT_KEYSTORE_TYPE);

        store.setDefault(PREF_KERBEROS_LOGGING_ENABLED, false);
        store.setDefault(PREF_KERBEROS_LOGGING_LEVEL, LEVEL.INFO.name());

        store.setDefault(PREF_KERBEROS_USER, "");
        store.setDefault(PREF_KERBEROS_KEYTAB_FILE, "");
        store.setDefault(PREF_KERBEROS_JDBC_IMPERSONATION_PARAM_FLAG, false);
        store.setDefault(PREF_KERBEROS_JDBC_IMPERSONATION_PARAM, "DelegationUID=" + JDBC_IMPERSONATION_PLACEHOLDER);
    }
}