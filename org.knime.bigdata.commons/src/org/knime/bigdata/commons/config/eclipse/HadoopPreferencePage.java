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

import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.knime.bigdata.commons.CommonsPlugin;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;

/**
 * @author Tobias Koetter, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
public class HadoopPreferencePage extends FieldEditorPreferencePage implements IWorkbenchPreferencePage {

    private static final String SSL_INFO =
        "The SSL settings are removed in KNIME 4.2 rom this preferences page and the new HDFS Connector uses the JVM SSL settings instead.";

    private FileFieldEditor m_coreSiteConf;
    private FileFieldEditor m_hdfsSiteConf;

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

            createInfoHeader(getFieldEditorParent(), SSL_INFO);
        }
    }

    private static void createInfoHeader(final Composite mainContainer, final String message) {
        Composite deprecationContainer = new Composite(mainContainer, SWT.NONE);
        deprecationContainer.setLayout(new GridLayout(2, false));
        deprecationContainer.setLayoutData(new GridData(SWT.FILL, SWT.NONE, true, false, 2, 1));

        Label image = new Label(deprecationContainer, SWT.NONE);
        image.setImage(mainContainer.getDisplay().getSystemImage(SWT.ICON_INFORMATION));
        image.setLayoutData(new GridData(SWT.NONE, SWT.FILL, true, true));

        Label text = new Label(deprecationContainer, SWT.NONE);
        text.setText(message);
        text.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, true));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean performOk() {
        final boolean result = super.performOk();
        // technically, this is not very threadsafe
        UserGroupUtil.initHadoopConfigurationAndUGI(UserGroupInformation.isSecurityEnabled());
        return result;
    }
}

