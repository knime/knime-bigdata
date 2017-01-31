/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.jface.preference.StringFieldEditor;
import org.eclipse.jface.util.IPropertyChangeListener;
import org.eclipse.jface.util.PropertyChangeEvent;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.knime.workbench.ui.preferences.LabelField;

import com.knime.bigdata.commons.CommonsPlugin;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class KerberosPreferencePage extends FieldEditorPreferencePage implements IWorkbenchPreferencePage {

    private BooleanFieldEditor m_jdbcParamFlag;
    private StringFieldEditor m_jdbcParam;

    /**
     * Creates a new kerberos preference page.
     */
    public KerberosPreferencePage() {
        super(FieldEditorPreferencePage.GRID);
        setDescription("Kerberos Configuration");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final IWorkbench workbench) {
        final IPreferenceStore prefStore = CommonsPlugin.getDefault().getPreferenceStore();
        setPreferenceStore(prefStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void createFieldEditors() {
        final StringFieldEditor kerberosUser = new StringFieldEditor(
            CommonPreferenceInitializer.PREF_KERBEROS_USER, "Keytab user:", getFieldEditorParent());
        addField(kerberosUser);

        final FileFieldEditor kerberosKeytabFile = new FileFieldEditor(
            CommonPreferenceInitializer.PREF_KERBEROS_KEYTAB_FILE, "Keytab file:", getFieldEditorParent());
        addField(kerberosKeytabFile);

        m_jdbcParamFlag = new BooleanFieldEditor(
            CommonPreferenceInitializer.PREF_KERBEROS_JDBC_IMPERSONATION_PARAM_FLAG,
            "Use JDBC impersonation parameter instead of Kerberos proxy user on KNIME Server",
            getFieldEditorParent());
        m_jdbcParam = new StringFieldEditor(
            CommonPreferenceInitializer.PREF_KERBEROS_JDBC_IMPERSONATION_PARAM, "JDBC impersonation parameter:",
            getFieldEditorParent());
        m_jdbcParamFlag.setPropertyChangeListener(new IPropertyChangeListener() {

            @Override
            public void propertyChange(final PropertyChangeEvent event) {
                updateFieldStatus();
            }
        });
        final LabelField label = new LabelField(getFieldEditorParent(),
                "The placeholder {1} in the impersonation parameter will be replaced by the KNIME workflow user.");
        updateFieldStatus();
        addField(m_jdbcParamFlag);
        addField(m_jdbcParam);
        addField(label);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initialize() {
        super.initialize();
        updateFieldStatus();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void propertyChange(final PropertyChangeEvent event) {
        super.propertyChange(event);
        if (event.getSource() == m_jdbcParamFlag) {
            updateFieldStatus();
        }
    }

    /**
     *
     */
    private void updateFieldStatus() {
        m_jdbcParam.setEnabled(m_jdbcParamFlag.getBooleanValue(), getFieldEditorParent());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void performDefaults() {
        super.performDefaults();
        updateFieldStatus();
    }
}
