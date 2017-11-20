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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
package com.knime.bigdata.commons.config.eclipse;

import org.eclipse.jface.preference.BooleanFieldEditor;
import org.eclipse.jface.preference.ComboFieldEditor;
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.jface.preference.IntegerFieldEditor;
import org.eclipse.jface.preference.StringFieldEditor;
import org.eclipse.jface.util.PropertyChangeEvent;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

import com.knime.bigdata.commons.CommonsPlugin;
import com.knime.bigdata.commons.config.CommonConfigContainer;

/**
 * @author Tobias Koetter, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
public class HadoopPreferencePage extends FieldEditorPreferencePage implements IWorkbenchPreferencePage {

    private FileFieldEditor m_coreSiteConf;
    private FileFieldEditor m_hdfsSiteConf;

    private BooleanFieldEditor m_enableTruststore;
    private ComboFieldEditor m_hostnameVerifier;
    private FileFieldEditor m_truststoreLocation;
    private StringFieldEditor m_truststorePassword;
    private ComboFieldEditor m_truststoreType;
    private IntegerFieldEditor m_truststoreReloadInterval;

    private BooleanFieldEditor m_enableKeystore;
    private FileFieldEditor m_keystoreLocation;
    private StringFieldEditor m_keystorePassword;
    private StringFieldEditor m_keystoreKeypassword;
    private ComboFieldEditor m_keystoreType;

    /**
     * Creates a new hadoop preference page.
     */
    public HadoopPreferencePage() {
        super(FieldEditorPreferencePage.GRID);

        if (CommonConfigContainer.getInstance().isHdfsSupported()) {
            setDescription("Hadoop Configuration");
        } else {
            setDescription("No Hadoop configuration available.");
        }
    }

    @Override
    public void init(final IWorkbench workbench) {
        final IPreferenceStore prefStore = CommonsPlugin.getDefault().getPreferenceStore();
        setPreferenceStore(prefStore);
    }

    @Override
    protected void createFieldEditors() {
        if (CommonConfigContainer.getInstance().isHdfsSupported()) {
            m_coreSiteConf = new FileFieldEditor(
                CommonPreferenceInitializer.PREF_CORE_SITE_FILE,
                "Custom core-site.xml file:", true, getFieldEditorParent());
            m_coreSiteConf.setFileExtensions(new String[] { "*.xml" });
            addField(m_coreSiteConf);

            m_hdfsSiteConf = new FileFieldEditor(
                CommonPreferenceInitializer.PREF_HDFS_SITE_FILE,
                "Custom hdfs-site.xml file:", true, getFieldEditorParent());
            m_hdfsSiteConf.setFileExtensions(new String[] { "*.xml" });
            addField(m_hdfsSiteConf);

            addSSLTruststoreEditors();
            addSSLKeystoreFields();
        }
    }

    @Override
    public void propertyChange(final PropertyChangeEvent event) {
        super.propertyChange(event);

        if (event.getSource().equals(m_enableTruststore)) {
            setTruststoreEnabled((boolean) event.getNewValue());
        }

        if (event.getSource().equals(m_enableKeystore)) {
            setKeystoreEnabled((boolean) event.getNewValue());
        }
    }

    @Override
    protected void performDefaults() {
        super.performDefaults();
        setTruststoreEnabled(m_enableTruststore.getBooleanValue());
        setKeystoreEnabled(m_enableKeystore.getBooleanValue());
    }

    /** Enable or disable truststore fields */
    private void setTruststoreEnabled(final boolean enabled) {
        m_hostnameVerifier.setEnabled(enabled, getFieldEditorParent());
        m_truststoreLocation.setEnabled(enabled, getFieldEditorParent());
        m_truststorePassword.setEnabled(enabled, getFieldEditorParent());
        m_truststoreType.setEnabled(enabled, getFieldEditorParent());
        m_truststoreReloadInterval.setEnabled(enabled, getFieldEditorParent());
    }

    private void addSSLTruststoreEditors() {
        m_enableTruststore = new BooleanFieldEditor(
            CommonPreferenceInitializer.PREF_TRUSTSTORE_ENABLE,
            "Use SSL truststore:", getFieldEditorParent());
        addField(m_enableTruststore);

        m_hostnameVerifier = new ComboFieldEditor(
            CommonPreferenceInitializer.PREF_TRUSTSTORE_HOSTNAME_VERIFIER,
            "Hostname verifier:",
            new String[][] {
                { "Default", "DEFAULT" },
                { "Default and localhost", "DEFAULT_AND_LOCALHOST" },
                { "Strict", "STRICT" },
                { "Strict IE6", "STRICT_IE6" },
                { "Allow all", "ALLOW_ALL" }
            },
            getFieldEditorParent());
        addField(m_hostnameVerifier);

        m_truststoreLocation = new FileFieldEditor(
            CommonPreferenceInitializer.PREF_TRUSTSTORE_LOCATION,
            "Truststore file:", true, getFieldEditorParent());
        m_truststoreLocation.setFileExtensions(new String[] { "*.jks", "*.jceks", "*.dks", "*.pkcs11", "*.pkcs12", "*" });
        addField(m_truststoreLocation);

        m_truststorePassword = new StringFieldEditor(
            CommonPreferenceInitializer.PREF_TRUSTSTORE_PASSWORD,
            "Truststore password:", getFieldEditorParent());
        m_truststorePassword.getTextControl(getFieldEditorParent()).setEchoChar('*');
        addField(m_truststorePassword);

        m_truststoreType = new ComboFieldEditor(CommonPreferenceInitializer.PREF_TRUSTSTORE_TYPE,
            "Tuststore type:",
            new String[][] {
                { "jks (default)", "jks" }, { "jceks", "jceks" }, { "dks", "dks" },
                { "pkcs11", "pkcs11" }, { "pkcs12", "pkcs12" }
            },
            getFieldEditorParent());
        addField(m_truststoreType);

        m_truststoreReloadInterval = new IntegerFieldEditor(
            CommonPreferenceInitializer.PREF_TRUSTSTORE_RELOAD_INTERVAL,
            "Truststore reload interval (ms):", getFieldEditorParent(), 10);
        m_truststoreReloadInterval.setValidRange(0, 100000000);
        addField(m_truststoreReloadInterval);

        setTruststoreEnabled(CommonConfigContainer.getInstance().hasSSLTruststoreConfig());
    }

    /** Enable or disable keystore fields */
    private void setKeystoreEnabled(final boolean enabled) {
        m_keystoreLocation.setEnabled(enabled, getFieldEditorParent());
        m_keystorePassword.setEnabled(enabled, getFieldEditorParent());
        m_keystoreKeypassword.setEnabled(enabled, getFieldEditorParent());
        m_keystoreType.setEnabled(enabled, getFieldEditorParent());
    }

    private void addSSLKeystoreFields() {
        m_enableKeystore = new BooleanFieldEditor(
            CommonPreferenceInitializer.PREF_KEYSTORE_ENABLE,
            "Use SSL keystore:", getFieldEditorParent());
        addField(m_enableKeystore);

        m_keystoreLocation = new FileFieldEditor(
            CommonPreferenceInitializer.PREF_KEYSTORE_LOCATION,
            "Keystore file:", true, getFieldEditorParent());
        m_keystoreLocation.setFileExtensions(new String[] { "*.jks", "*.jceks", "*.dks", "*.pkcs11", "*.pkcs12", "*" });
        addField(m_keystoreLocation);

        m_keystorePassword = new StringFieldEditor(
            CommonPreferenceInitializer.PREF_KEYSTORE_PASSWORD,
            "Keystore password:", getFieldEditorParent());
        m_keystorePassword.getTextControl(getFieldEditorParent()).setEchoChar('*');
        addField(m_keystorePassword);

        m_keystoreKeypassword = new StringFieldEditor(
            CommonPreferenceInitializer.PREF_KEYSTORE_KEYPASSWORD,
            "Keystore key password:", getFieldEditorParent());
        m_keystoreKeypassword.getTextControl(getFieldEditorParent()).setEchoChar('*');
        addField(m_keystoreKeypassword);

        m_keystoreType = new ComboFieldEditor(CommonPreferenceInitializer.PREF_KEYSTORE_TYPE,
            "Keystore type:",
            new String[][] {
                { "jks (default)", "jks" }, { "jceks", "jceks" }, { "dks", "dks" },
                { "pkcs11", "pkcs11" }, { "pkcs12", "pkcs12" }
            },
            getFieldEditorParent());
        addField(m_keystoreType);

        setKeystoreEnabled(CommonConfigContainer.getInstance().hasSSLKeystoreConfig());
    }
}

