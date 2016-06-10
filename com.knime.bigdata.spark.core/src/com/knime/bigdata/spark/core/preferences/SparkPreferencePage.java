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
package com.knime.bigdata.spark.core.preferences;

import java.util.Arrays;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.util.IPropertyChangeListener;
import org.eclipse.jface.util.PropertyChangeEvent;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.eclipse.ui.PlatformUI;

import com.knime.bigdata.spark.core.SparkPlugin;
import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * @author Tobias Koetter, University of Konstanz
 * @author Sascha Wolke, KNIME.com
 */
public class SparkPreferencePage extends PreferencePage
        implements IWorkbenchPreferencePage, IPropertyChangeListener, Listener {

    private Text m_jobServerUrl;
    private Button m_withoutAuthentication;
    private Button m_withAuthentication;
    private Text m_username;
    private Text m_password;
    private Spinner m_jobTimeout;
    private Spinner m_jobCheckFrequency;

    private Combo m_sparkVersion;
    private Text m_contextName;
    private Button m_deleteSparkObjectsOnDispose;
    private Button m_sparkJobLevel[];
    private Button m_overrideSettings;
    private Text m_customSettings;

    private Button m_verboseLogging;

    private boolean m_contextResetInfoDisplayed;


    /**
     * Creates a new spark preference page.
     */
    public SparkPreferencePage() {
        super();
        setDescription("KNIME Spark Executor Preferences");
    }

    @Override
    public void init(final IWorkbench workbench) {
        final IPreferenceStore prefStore = SparkPlugin.getDefault().getPreferenceStore();
        setPreferenceStore(prefStore);
        prefStore.addPropertyChangeListener(this);
    }

    @Override
    protected Control createContents(final Composite parent) {
        Composite mainContainer = new Composite(parent, SWT.NONE);
        GridLayout gl = new GridLayout(1, true);
        mainContainer.setLayout(gl);
        GridData mainContainerLayoutData = new GridData(SWT.FILL, SWT.FILL, true, true);
        mainContainerLayoutData.widthHint = 300;
        mainContainer.setLayoutData(mainContainerLayoutData);


        /////////////// Connection settings ///////////////
        Group connectionSettings = new Group(mainContainer, SWT.NONE);
        connectionSettings.setText("Connection settings:");
        connectionSettings.setLayout(new GridLayout(1, false));
        connectionSettings.setLayoutData(new GridData(SWT.FILL, SWT.NONE, true, false));

        m_jobServerUrl = createTextWithLabel(connectionSettings, "Job server URL:");

        Composite credentialsContainer = new Composite(connectionSettings, SWT.NONE);
        credentialsContainer.setLayout(new GridLayout(2, false));
        credentialsContainer.setLayoutData(new GridData(SWT.FILL, SWT.NONE, true, false));

        new Label(credentialsContainer, SWT.LEFT).setText("Credentials:");
        m_withoutAuthentication = new Button(credentialsContainer, SWT.RADIO);
        m_withoutAuthentication.setText("No authentication required.");
        m_withoutAuthentication.addListener(SWT.Selection, this);

        new Label(credentialsContainer, SWT.NONE); // dummy
        m_withAuthentication = new Button(credentialsContainer, SWT.RADIO);
        m_withAuthentication.setText("With authentication:");
        m_withAuthentication.addListener(SWT.Selection, this);

        GridData userPasswordGroupLayoutData = new GridData(SWT.FILL, SWT.NONE, true, false);
        userPasswordGroupLayoutData.horizontalIndent = 20;
        new Label(credentialsContainer, SWT.NONE); // dummy
        m_username = createTextWithLabel(credentialsContainer, "Username:");
        m_username.getParent().setLayoutData(userPasswordGroupLayoutData);
        new Label(credentialsContainer, SWT.NONE); // dummy
        m_password = createTextWithLabel(credentialsContainer, "Password:");
        m_password.setEchoChar('*');
        m_password.getParent().setLayoutData(userPasswordGroupLayoutData);

        m_jobTimeout = createSpinnerWithLabel(connectionSettings, "Job timeout in seconds:", 0, 10);

        m_jobCheckFrequency = createSpinnerWithLabel(connectionSettings, "Job check frequency in seconds:", 0, 1);


        /////////////// Context settings ///////////////
        Group contextSettings = new Group(mainContainer, SWT.NONE);
        contextSettings.setText("Context settings:");
        contextSettings.setLayout(new GridLayout(1, false));
        contextSettings.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

        Composite sparkVersionContainer = createGridLayoutContainer(contextSettings, 2);
        Label sparkVersionLabel = new Label(sparkVersionContainer, SWT.NONE);
        sparkVersionLabel.setText("Spark version:");
        m_sparkVersion = new Combo(sparkVersionContainer, SWT.READ_ONLY);
        m_sparkVersion.setItems(SparkVersion.getAllVersionLabels());

        m_contextName = createTextWithLabel(contextSettings, "Context name:");

        m_deleteSparkObjectsOnDispose = new Button(contextSettings, SWT.CHECK);
        m_deleteSparkObjectsOnDispose.setText("Delete Spark objects on dispose.");

        Composite logLevelContainer = createGridLayoutContainer(contextSettings, SparkPreferenceInitializer.ALL_LOG_LEVELS.length + 1);
        new Label(logLevelContainer, SWT.NONE).setText("Spark job log level:");
        m_sparkJobLevel = new Button[SparkPreferenceInitializer.ALL_LOG_LEVELS.length];
        for (int i = 0; i < SparkPreferenceInitializer.ALL_LOG_LEVELS.length; i++) {
            m_sparkJobLevel[i] = new Button(logLevelContainer, SWT.RADIO);
            m_sparkJobLevel[i].setText(SparkPreferenceInitializer.ALL_LOG_LEVELS[i]);
            m_sparkJobLevel[i].addListener(SWT.Selection, this);
        }

        m_overrideSettings = new Button(contextSettings, SWT.CHECK);
        m_overrideSettings.setText("Override spark settings:");
        m_overrideSettings.addListener(SWT.Selection, this);
        m_customSettings = new Text(contextSettings, SWT.BORDER | SWT.MULTI);
        GridData customSettingsLayoutData = new GridData(SWT.FILL, SWT.FILL, true, true);
        customSettingsLayoutData.heightHint = 100;
        m_customSettings.setLayoutData(customSettingsLayoutData);


        /////////////// KNIME settings ///////////////
        Group knimeSettings = new Group(mainContainer, SWT.NONE);
        knimeSettings.setText("KNIME settings:");
        knimeSettings.setLayout(new GridLayout(1, true));
        knimeSettings.setLayoutData(new GridData(SWT.FILL, SWT.NONE, true, false));

        m_verboseLogging = new Button(knimeSettings, SWT.CHECK);
        m_verboseLogging.setText("Enable verbose logging.");

        loadPreferencesIntoFields();

        return mainContainer;
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

    private Spinner createSpinnerWithLabel(final Composite parent, final String description, final int minimum, final int increment) {
        Composite group = new Composite(parent, SWT.NONE);
        group.setLayout(new GridLayout(2, false));
        group.setLayoutData(new GridData(SWT.FILL, SWT.NONE, true, false));
        Label label = new Label(group, SWT.LEFT);
        label.setText(description);
        Spinner spinner = new Spinner(group, SWT.BORDER);
        spinner.setMinimum(minimum);
        spinner.setIncrement(increment);
        spinner.setMaximum(1000000);
        GridData spinnerLayoutData = new GridData();
        spinnerLayoutData.widthHint = 100;
        spinner.setLayoutData(spinnerLayoutData);
        return spinner;
    }

    private Composite createGridLayoutContainer(final Composite parent, final int numColumns) {
        Composite container = new Composite(parent, SWT.NONE);
        container.setLayout(new GridLayout(numColumns, false));
        return container;
    }

    /** Load preferences from store into fields. */
    private void loadPreferencesIntoFields() {
        IPreferenceStore prefs = getPreferenceStore();

        m_contextResetInfoDisplayed = false;

        m_jobServerUrl.setText(prefs.getString(SparkPreferenceInitializer.PREF_JOB_SERVER_URL));
        m_withoutAuthentication.setSelection(!prefs.getBoolean(SparkPreferenceInitializer.PREF_AUTHENTICATION));
        m_withAuthentication.setSelection(prefs.getBoolean(SparkPreferenceInitializer.PREF_AUTHENTICATION));
        m_username.setText(prefs.getString(SparkPreferenceInitializer.PREF_USER_NAME));
        m_password.setText(prefs.getString(SparkPreferenceInitializer.PREF_PWD));
        setUserPasswordFieldEnabled(prefs.getBoolean(SparkPreferenceInitializer.PREF_AUTHENTICATION));
        m_jobTimeout.setSelection(prefs.getInt(SparkPreferenceInitializer.PREF_JOB_TIMEOUT));
        m_jobCheckFrequency.setSelection(prefs.getInt(SparkPreferenceInitializer.PREF_JOB_CHECK_FREQUENCY));

        setSparkVersionField(prefs.getString(SparkPreferenceInitializer.PREF_SPARK_VERSION));
        m_contextName.setText(prefs.getString(SparkPreferenceInitializer.PREF_CONTEXT_NAME));
        selectSparkJobLogLevelButton(prefs.getString(SparkPreferenceInitializer.PREF_JOB_LOG_LEVEL));
        m_overrideSettings.setSelection(prefs.getBoolean(SparkPreferenceInitializer.PREF_OVERRIDE_SPARK_SETTINGS));
        m_customSettings.setText(prefs.getString(SparkPreferenceInitializer.PREF_CUSTOM_SPARK_SETTINGS));
        m_customSettings.setEnabled(prefs.getBoolean(SparkPreferenceInitializer.PREF_OVERRIDE_SPARK_SETTINGS));

        m_verboseLogging.setSelection(prefs.getBoolean(SparkPreferenceInitializer.PREF_VERBOSE_LOGGING));
    }

    @Override
    protected void performDefaults() {
        IPreferenceStore prefs = getPreferenceStore();

        m_jobServerUrl.setText(prefs.getDefaultString(SparkPreferenceInitializer.PREF_JOB_SERVER_URL));
        m_withoutAuthentication.setSelection(!prefs.getDefaultBoolean(SparkPreferenceInitializer.PREF_AUTHENTICATION));
        m_withAuthentication.setSelection(prefs.getDefaultBoolean(SparkPreferenceInitializer.PREF_AUTHENTICATION));
        m_username.setText(prefs.getDefaultString(SparkPreferenceInitializer.PREF_USER_NAME));
        m_password.setText(prefs.getDefaultString(SparkPreferenceInitializer.PREF_PWD));
        setUserPasswordFieldEnabled(prefs.getDefaultBoolean(SparkPreferenceInitializer.PREF_AUTHENTICATION));
        m_jobTimeout.setSelection(prefs.getDefaultInt(SparkPreferenceInitializer.PREF_JOB_TIMEOUT));
        m_jobCheckFrequency.setSelection(prefs.getDefaultInt(SparkPreferenceInitializer.PREF_JOB_CHECK_FREQUENCY));

        setSparkVersionField(prefs.getDefaultString(SparkPreferenceInitializer.PREF_SPARK_VERSION));
        m_contextName.setText(prefs.getDefaultString(SparkPreferenceInitializer.PREF_CONTEXT_NAME));
        selectSparkJobLogLevelButton(prefs.getDefaultString(SparkPreferenceInitializer.PREF_JOB_LOG_LEVEL));
        m_overrideSettings.setSelection(prefs.getDefaultBoolean(SparkPreferenceInitializer.PREF_OVERRIDE_SPARK_SETTINGS));
        m_customSettings.setText(prefs.getDefaultString(SparkPreferenceInitializer.PREF_CUSTOM_SPARK_SETTINGS));
        m_customSettings.setEnabled(prefs.getDefaultBoolean(SparkPreferenceInitializer.PREF_OVERRIDE_SPARK_SETTINGS));

        m_verboseLogging.setSelection(prefs.getDefaultBoolean(SparkPreferenceInitializer.PREF_VERBOSE_LOGGING));

        super.performDefaults();
    }

    @Override
    public boolean performOk() {
        IPreferenceStore prefs = getPreferenceStore();

        prefs.setValue(SparkPreferenceInitializer.PREF_JOB_SERVER_URL, m_jobServerUrl.getText());
        prefs.setValue(SparkPreferenceInitializer.PREF_AUTHENTICATION, m_withAuthentication.getSelection());
        prefs.setValue(SparkPreferenceInitializer.PREF_USER_NAME, m_username.getText());
        prefs.setValue(SparkPreferenceInitializer.PREF_PWD, m_password.getText());
        prefs.setValue(SparkPreferenceInitializer.PREF_JOB_TIMEOUT, m_jobTimeout.getSelection());
        prefs.setValue(SparkPreferenceInitializer.PREF_JOB_CHECK_FREQUENCY, m_jobCheckFrequency.getSelection());

        if (m_sparkVersion.getSelectionIndex() > 0) {
            prefs.setValue(SparkPreferenceInitializer.PREF_SPARK_VERSION, SparkVersion.getAllVersionLabels()[m_sparkVersion.getSelectionIndex()]);
        } else {
            prefs.setToDefault(SparkPreferenceInitializer.PREF_SPARK_VERSION);
        }
        prefs.setValue(SparkPreferenceInitializer.PREF_CONTEXT_NAME, m_contextName.getText());

        prefs.setValue(SparkPreferenceInitializer.PREF_DELETE_OBJECTS_ON_DISPOSE, m_deleteSparkObjectsOnDispose.getSelection());

        String logLevel = null;
        for (int i = 0; i < m_sparkJobLevel.length; i++) {
            if (m_sparkJobLevel[i].getSelection()) {
                logLevel = SparkPreferenceInitializer.ALL_LOG_LEVELS[i];
                break;
            }
        }
        if (logLevel != null) {
            prefs.setValue(SparkPreferenceInitializer.PREF_JOB_LOG_LEVEL, logLevel);
        } else {
            prefs.setToDefault(SparkPreferenceInitializer.PREF_JOB_LOG_LEVEL);
        }

        prefs.setValue(SparkPreferenceInitializer.PREF_OVERRIDE_SPARK_SETTINGS, m_overrideSettings.getSelection());
        prefs.setValue(SparkPreferenceInitializer.PREF_CUSTOM_SPARK_SETTINGS, m_customSettings.getText());

        prefs.setValue(SparkPreferenceInitializer.PREF_VERBOSE_LOGGING, m_verboseLogging.getSelection());

        return true;
    }

    @Override
    public void handleEvent(final Event event) {
        if (event.widget.equals(m_withoutAuthentication) && m_withoutAuthentication.getSelection()) {
            setUserPasswordFieldEnabled(false);
        } else if (event.widget.equals(m_withAuthentication) && m_withAuthentication.getSelection()) {
            setUserPasswordFieldEnabled(true);
        }

        if (event.widget.equals(m_overrideSettings)) {
            m_customSettings.setEnabled(m_overrideSettings.getSelection());
        }
    }

    /** Enables oder disables username and password field. */
    private void setUserPasswordFieldEnabled(final boolean enabled) {
        m_username.setEnabled(enabled);
        m_password.setEnabled(enabled);
    }

    /** Selects given version in spark version combo box. */
    private void setSparkVersionField(final String version) {
        int index = m_sparkVersion.indexOf(SparkVersion.getVersion(version).getLabel());
        if (index >= 0 && index < m_sparkVersion.getItemCount()) {
            m_sparkVersion.select(index);
        }
    }

    /** Selects given spark job log level radio button. */
    private void selectSparkJobLogLevelButton(final String level) {
        for (int i = 0; i < SparkPreferenceInitializer.ALL_LOG_LEVELS.length; i++) {
            m_sparkJobLevel[i].setSelection(SparkPreferenceInitializer.ALL_LOG_LEVELS[i].equals(level));
        }
    }

    @Override
    public void propertyChange(final PropertyChangeEvent event) {
        final String property = event.getProperty();
        System.out.println("Found change: " + property);

        try {
            // context id has changed
            if (property.equals(SparkPreferenceInitializer.PREF_JOB_SERVER_URL) || property.equals(SparkPreferenceInitializer.PREF_CONTEXT_NAME)) {
                SparkContextManager.getDefaultSparkContext().ensureDestroyed();
                SparkContextManager.refreshDefaultContext();
                showContextResetInfo();

            // context setting has changed
            } else if (Arrays.binarySearch(SparkPreferenceInitializer.PREF_ALL_CONTEXT_SETTINGS, event.getProperty()) >= 0) {
                SparkContextManager.getDefaultSparkContext().ensureDestroyed();
                showContextResetInfo();
            }

        } catch (KNIMESparkException e) {
            MessageDialog.openError(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(),
                "Spark context reconfiguration failed",
                "Failed to reconfigure spark context. New settings require restart to take affect.");
        }
    }

    private void showContextResetInfo() {
        if (!m_contextResetInfoDisplayed) {
            MessageDialog.openInformation(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(),
                "Spark context destroyed.",
                "Spark context settings change detected. Old context has been destroyed.");
            m_contextResetInfoDisplayed = true;
        }
    }
}