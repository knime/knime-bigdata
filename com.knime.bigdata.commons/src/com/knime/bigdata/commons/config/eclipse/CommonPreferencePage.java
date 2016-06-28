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

import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.jface.preference.StringFieldEditor;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

import com.knime.bigdata.commons.CommonsPlugin;
import com.knime.bigdata.commons.config.CommonConfigContainer;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class CommonPreferencePage extends FieldEditorPreferencePage implements IWorkbenchPreferencePage {

    /**
     * Creates a new big data common preference page.
     */
    public CommonPreferencePage() {
        super(FieldEditorPreferencePage.GRID);
    }

    @Override
    public void init(final IWorkbench workbench) {
        final IPreferenceStore prefStore = CommonsPlugin.getDefault().getPreferenceStore();
        setPreferenceStore(prefStore);
    }

    @Override
    protected void createFieldEditors() {
        // HDFS settings
        if (CommonConfigContainer.getInstance().isHdfsSupported()) {
            FileFieldEditor coreSiteConf = new FileFieldEditor(
                CommonPreferenceInitializer.PREF_CORE_SITE_FILE,
                "Custom core-site.xml file:", true, getFieldEditorParent());
            coreSiteConf.setFileExtensions(new String[] { "*.xml" });
            addField(coreSiteConf);

            FileFieldEditor hdfsSiteConf = new FileFieldEditor(
                CommonPreferenceInitializer.PREF_HDFS_SITE_FILE,
                "Custom hdfs-site.xml file:", true, getFieldEditorParent());
            hdfsSiteConf.setFileExtensions(new String[] { "*.xml" });
            addField(hdfsSiteConf);
        }

        // Kerberos settings
        StringFieldEditor kerberosUser = new StringFieldEditor(
            CommonPreferenceInitializer.PREF_KERBEROS_USER, "Custom kerberos user:", getFieldEditorParent());
        addField(kerberosUser);

        FileFieldEditor kerberosKeytabFile = new FileFieldEditor(
            CommonPreferenceInitializer.PREF_KERBEROS_KEYTAB_FILE, "Custom Kerberos keytab:", getFieldEditorParent());
        addField(kerberosKeytabFile);
    }
}
