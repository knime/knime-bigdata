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
package com.knime.bigdata.spark.preferences;

import java.util.Arrays;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.preference.BooleanFieldEditor;
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.IntegerFieldEditor;
import org.eclipse.jface.preference.RadioGroupFieldEditor;
import org.eclipse.jface.preference.StringFieldEditor;
import org.eclipse.jface.util.IPropertyChangeListener;
import org.eclipse.jface.util.PropertyChangeEvent;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.eclipse.ui.PlatformUI;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 *
 * @author Tobias Koetter, University of Konstanz
 */
public class SparkPreferencePage extends FieldEditorPreferencePage
        implements IWorkbenchPreferencePage {

    /**
     * Creates a new preference page.
     */
    public SparkPreferencePage() {
        super(GRID);
        setPreferenceStore(SparkPlugin.getDefault().getPreferenceStore());
        setDescription("KNIME Big Data Connector for Spark preferences");
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void createFieldEditors() {
        final Composite parent = getFieldEditorParent();
            final StringFieldEditor jobServerUrl = new StringFieldEditor(
                    SparkPreferenceInitializer.PREF_JOB_SERVER, "Job server url: ", parent);
            jobServerUrl.setEmptyStringAllowed(false);
//            final StringFieldEditor jobServerProtocol = new StringFieldEditor(
//                SparkPreferenceInitializer.PREF_JOB_SERVER_PROTOCOL, "Job server protocol (http/https): ", parent);
//            jobServerProtocol.setEmptyStringAllowed(false);
            final String[] protocols = KNIMESparkContext.getSupportedProtocols();
            final String[][] options = new String[protocols.length][2];
            for (int i = 0, length = protocols.length; i < length; i++) {
                options[i][0] = protocols[i];
                options[i][1] = protocols[i];
            }
            final RadioGroupFieldEditor jobServerProtocol = new RadioGroupFieldEditor(SparkPreferenceInitializer.PREF_JOB_SERVER_PROTOCOL,
                "Job server protocol", 1, options, parent, false);
            final IntegerFieldEditor jobServerPort = new IntegerFieldEditor(
                SparkPreferenceInitializer.PREF_JOB_SERVER_PORT, "Job server port: ", parent);
            jobServerPort.setValidRange(0, Integer.MAX_VALUE);
            final StringFieldEditor userName = new StringFieldEditor(
                SparkPreferenceInitializer.PREF_USER_NAME, "User name: ", parent);
            userName.setEmptyStringAllowed(false);
            final StringFieldEditor pwd = new StringFieldEditor(
                SparkPreferenceInitializer.PREF_PWD, "Password: ", parent);
            pwd.getTextControl(parent).setEchoChar('*');

            final StringFieldEditor contextName = new StringFieldEditor(
                SparkPreferenceInitializer.PREF_CONTEXT_NAME, "Context name: ", parent);
            contextName.setEmptyStringAllowed(false);

            //We might want to hide these settings here since we support only one context anyway
            final IntegerFieldEditor numCPUCores = new IntegerFieldEditor(
                    SparkPreferenceInitializer.PREF_NUM_CPU_CORES, "Number of CPU cores: ", parent);
            numCPUCores.setValidRange(1, Integer.MAX_VALUE);
            final StringFieldEditor memPerNode = new StringFieldEditor(
                SparkPreferenceInitializer.PREF_MEM_PER_NODE, "Memory per node: ", parent);
            memPerNode.setEmptyStringAllowed(false);

            final IntegerFieldEditor jobTimeout = new IntegerFieldEditor(
                SparkPreferenceInitializer.PREF_JOB_TIMEOUT, "Job timeout in seconds: ", parent);
            jobTimeout.setValidRange(1, Integer.MAX_VALUE);
            final IntegerFieldEditor jobCheckFrequency = new IntegerFieldEditor(
                SparkPreferenceInitializer.PREF_JOB_CHECK_FREQUENCY, "Job timeout check frequency in seconds: ", parent);
            jobCheckFrequency.setValidRange(1, Integer.MAX_VALUE);
            final BooleanFieldEditor deleteRDDsOnDispose = new BooleanFieldEditor(
                SparkPreferenceInitializer.PREF_DELETE_RDDS_ON_DISPOSE, "Delete Spark RDDs on dispose", parent);
            final BooleanFieldEditor verboseLogging = new BooleanFieldEditor(
                SparkPreferenceInitializer.PREF_VERBOSE_LOGGING, "Enable verbose logging", parent);

            addField(jobServerUrl);
            addField(jobServerProtocol);
            addField(jobServerPort);

            addField(userName);
            addField(pwd);

            addField(contextName);
            addField(numCPUCores);
            addField(memPerNode);

            addField(jobTimeout);
            addField(jobCheckFrequency);
            addField(deleteRDDsOnDispose);
            addField(verboseLogging);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final IWorkbench workbench) {
        getPreferenceStore().addPropertyChangeListener(new IPropertyChangeListener() {
            @Override
            public void propertyChange(final PropertyChangeEvent event) {
                final String[] restartProps = new String[] {SparkPreferenceInitializer.PREF_CONTEXT_NAME,
                    SparkPreferenceInitializer.PREF_MEM_PER_NODE,  SparkPreferenceInitializer.PREF_NUM_CPU_CORES};
                final String property = event.getProperty();
                if (Arrays.binarySearch(restartProps, property) >= 0) {
//                    if(MessageDialog.openConfirm(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(), "Information", "New settings will be applied after a restart.\nRestart now?")) {
//                        PlatformUI.getWorkbench().restart();
//                    }
                    //TODO: What do we do when the context changes. Shall we delete the old context?
                    MessageDialog.openInformation(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(),
                        "Spark Context Settings",
                        "Spark context changes only take effect after resetting the Spark nodes");
                }
            }
        });
    }
}
