/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
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
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package com.knime.bigdata.scripting.nodes.python.hive;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.io.IOException;
import java.util.Collection;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.SwingConstants;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.code.generic.templates.SourceCodeTemplatesPanel;
import org.knime.code.python.PythonSourceCodePanel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python.kernel.SQLEditorObjectWriter;

import com.knime.bigdata.hive.utility.HiveUtility;

/**
 * <code>NodeDialog</code> for the node.
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
class PythonScriptHiveNodeDialog extends NodeDialogPane {

	private final PythonSourceCodePanel m_sourceCodePanel;
	private final SourceCodeTemplatesPanel m_templatesPanel;
    private final RemoteFileChooserPanel m_target;

	/**
	 * Create the dialog for this node.
	 */
	protected PythonScriptHiveNodeDialog() {
		m_sourceCodePanel = new PythonSourceCodePanel(PythonScriptHiveNodeConfig.getVariableNames());
		m_templatesPanel = new SourceCodeTemplatesPanel(m_sourceCodePanel, "python-script");
		m_target = new RemoteFileChooserPanel(getPanel(), "", false, "targetHistory", RemoteFileChooser.SELECT_DIR,
	                createFlowVariableModel("target", FlowVariable.Type.STRING), null);
		final JPanel sourcePanel = new JPanel(new GridBagLayout());
		GridBagConstraints gc = new GridBagConstraints();
		gc.fill = GridBagConstraints.BOTH;
		gc.gridx = 0;
		gc.gridy = 0;
		gc.ipady = 5;
		sourcePanel.add(new JLabel("Target folder"));
		gc.gridx++;
		gc.weightx = 1;
		gc.weighty = 0;
		sourcePanel.add(m_target.getPanel(), gc);
		gc.gridx = 0;
		gc.gridy++;
		gc.gridwidth = 2;
		sourcePanel.add(new JSeparator(SwingConstants.HORIZONTAL), gc);
		gc.ipady = 0;
		gc.gridy++;
		gc.weightx = 1;
        gc.weighty = 1;
		sourcePanel.add(m_sourceCodePanel, gc);
		addTab("Script", sourcePanel, false);
		addTab("Templates", m_templatesPanel, true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
		final PythonScriptHiveNodeConfig config = new PythonScriptHiveNodeConfig();
		m_sourceCodePanel.saveSettingsTo(config);
		config.setTargetFolder(m_target.getSelection());
		config.saveTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
			throws NotConfigurableException {
	    HiveUtility.LICENSE_CHECKER.checkLicenseInDialog();
		if (specs == null || specs.length < 2 || specs[0] == null || specs[1] == null) {
			throw new NotConfigurableException("Please connect the node");
		}
		final PythonScriptHiveNodeConfig config = new PythonScriptHiveNodeConfig();
		config.loadFromInDialog(settings);
		m_sourceCodePanel.loadSettingsFrom(config, specs);
		m_sourceCodePanel.updateFlowVariables(getAvailableFlowVariables().values().toArray(
				new FlowVariable[getAvailableFlowVariables().size()]));
		ConnectionInformation remoteFileInfo = ((ConnectionInformationPortObjectSpec)specs[0]).getConnectionInformation();
        m_target.setConnectionInformation(remoteFileInfo);
        m_target.setSelection(config.getTargetFolder());
		final DatabasePortObjectSpec dbSpec = (DatabasePortObjectSpec) specs[1];
		try {
			final CredentialsProvider cp = getCredentialsProvider();
			final DatabaseQueryConnectionSettings connInfo = dbSpec.getConnectionSettings(cp);
			final Collection<String> jars = PythonScriptHiveNodeModel.getJars(connInfo);
            final SQLEditorObjectWriter sqlObject = new SQLEditorObjectWriter(
					PythonScriptHiveNodeConfig.getVariableNames().getGeneralInputObjects()[0],
					connInfo, cp, jars);
			m_sourceCodePanel.updateData(sqlObject);
		} catch (final InvalidSettingsException|IOException e) {
			throw new NotConfigurableException(e.getMessage(), e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean closeOnESC() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onOpen() {
		m_sourceCodePanel.open();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onClose() {
		m_sourceCodePanel.close();
	}

}
