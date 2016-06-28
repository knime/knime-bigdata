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

import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

import com.knime.bigdata.commons.CommonsPlugin;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class HadoopPreferencePage extends PreferencePage implements IWorkbenchPreferencePage {

    //TODO: Use this preference page as parent for all other big data preference pages e.g. Spark

    private Text m_hadoopFile;

    /**
     * Creates a new spark preference page.
     */
    public HadoopPreferencePage() {
        super();
        setDescription("KNIME Big Data Extensions");
    }

    @Override
    public void init(final IWorkbench workbench) {
        final IPreferenceStore prefStore = CommonsPlugin.getDefault().getPreferenceStore();
        setPreferenceStore(prefStore);
    }

    @Override
    protected Control createContents(final Composite parent) {
        Composite mainContainer = new Composite(parent, SWT.NONE);
        GridLayout gl = new GridLayout(1, true);
        mainContainer.setLayout(gl);
        GridData mainContainerLayoutData = new GridData(SWT.FILL, SWT.FILL, true, true);
        mainContainerLayoutData.widthHint = 300;
        mainContainer.setLayoutData(mainContainerLayoutData);
        m_hadoopFile = createTextWithLabel(mainContainer, "Hadoop config file:");
        //TODO:  Show the hadoop config file option only if the hdfs file handling is enable?
//        HadoopConfigContainer.getInstance().isHdfsSupported()

        loadPreferencesIntoFields();

        return mainContainer;
    }

    /** Load preferences from store into fields. */
    private void loadPreferencesIntoFields() {
        IPreferenceStore prefs = getPreferenceStore();
        m_hadoopFile.setText(prefs.getString(HadoopPreferenceInitializer.PREF_HADOOP_CONF));
    }

    @Override
    protected void performDefaults() {
        IPreferenceStore prefs = getPreferenceStore();

        m_hadoopFile.setText(prefs.getDefaultString(HadoopPreferenceInitializer.PREF_HADOOP_CONF));
        setErrorMessage(null);
        setValid(true);

        super.performDefaults();
    }

    @Override
    public boolean performOk() {
        IPreferenceStore prefs = getPreferenceStore();

        prefs.setValue(HadoopPreferenceInitializer.PREF_HADOOP_CONF, m_hadoopFile.getText());
        return true;
    }

    private Text createTextWithLabel(final Composite parent, final String description) {
        Composite group = new Composite(parent, SWT.NONE);
        group.setLayout(new GridLayout(2, false));
        group.setLayoutData(new GridData(SWT.FILL, SWT.NONE, true, false));
        Label label = new Label(group, SWT.LEFT);
        label.setText(description);
        Text text = new Text(group, SWT.BORDER);
        text.setLayoutData(new GridData(SWT.FILL, SWT.NONE, true, false));
        return text;
    }
}
