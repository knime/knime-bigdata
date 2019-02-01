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
 *
 * History
 *   24.11.2011 (hofer): created
 */
package org.knime.bigdata.spark.node.scripting.python;

import java.io.IOException;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.folding.Fold;
import org.fife.ui.rsyntaxtextarea.folding.FoldManager;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkDocument;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkHelper;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkHelperRegistry;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkSourceCodePanel;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

/**
 * The dialog that is used in the PySpark scripting nodes.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class PySparkNodeDialog extends DataAwareNodeDialogPane{

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PySparkNodeDialog.class);

    private final PySparkSourceCodePanel m_sourceCodePanel;

    private PySparkNodeConfig m_config;

    private SparkVersion m_sparkVersion;

    private int m_inCount;

    private int m_outCount;

    /**
     * Create a new Dialog.
     *
     * @param inCount the number of inputs
     * @param outCount the number of outputs
     */
    public PySparkNodeDialog(final int inCount, final int outCount) {
        m_inCount = inCount;
        m_outCount = outCount;
        m_sparkVersion = KNIMEConfigContainer.getSparkVersion();
        PySparkHelper helper = getHelper();
        m_config = new PySparkNodeConfig(m_inCount, m_outCount);
        m_sourceCodePanel = new PySparkSourceCodePanel(m_config.getVariableNames(), (PySparkDocument)m_config.getDoc());
        try {
            m_sourceCodePanel.setPySparkPath(helper.getLocalPySparkPath());
        } catch (IOException e) {
           LOGGER.error("Could not obtain PySpark Path", e);
        }
        addTab("Script", m_sourceCodePanel, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_sourceCodePanel.saveSettingsTo(m_config);
        PySparkHelper helper = getHelper();
        helper.checkUDF((PySparkDocument)m_config.getDoc(), m_outCount);
        m_config.saveTo(settings);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        if (specs == null || specs.length < 1 || specs[0] == null) {
            m_sparkVersion = KNIMEConfigContainer.getSparkVersion();
            if (!PySparkHelperRegistry.getInstance().supportsVersion(m_sparkVersion)) {
                throw new NotConfigurableException(
                    String.format("PySpark is not supported for Spark version %s.", m_sparkVersion));
            }

        } else {
            m_sparkVersion = SparkContextUtil.getSparkVersion(((SparkContextProvider)specs[0]).getContextID());
        }
        PySparkHelper helper = getHelper();
        try {
            m_sourceCodePanel.setPySparkPath(helper.getLocalPySparkPath());
        } catch (IOException e) {
           LOGGER.error("Could not obtain PySpark path.", e);
        }
        PySparkNodeConfig config = new PySparkNodeConfig(m_inCount, m_outCount, helper);
        config.loadFromInDialog(settings);
        m_sourceCodePanel.loadSettingsFrom(config, specs);
        m_sourceCodePanel.updateFlowVariables(
            getAvailableFlowVariables().values().toArray(new FlowVariable[getAvailableFlowVariables().size()]));
        config.setDoc((PySparkDocument)m_sourceCodePanel.getEditor().getDocument());
        m_config = config;
        m_sourceCodePanel.updatePortObjects(null);

    }

    private PySparkHelper getHelper() {
        return PySparkHelperRegistry.getInstance().getHelper(m_sparkVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
        final PortObject[] input) throws NotConfigurableException {
        final PortObjectSpec[] specs = new PortObjectSpec[input.length];
        for (int i = 0; i < specs.length; i++) {
            specs[i] = input[i] == null ? null : input[i].getSpec();
        }
        loadSettingsFrom(settings, specs);
        m_sourceCodePanel.updatePortObjects(input);
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
        RSyntaxTextArea editor = m_sourceCodePanel.getEditor();
        editor.requestFocus();
        editor.requestFocusInWindow();
        // reset style which causes a recreation of the popup window with
        // the side effect, that all folds are recreated, so that we must collapse
        editor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_NONE);
        editor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_PYTHON);
        // collapse all folds
        FoldManager foldManager = editor.getFoldManager();
        int foldCount = foldManager.getFoldCount();
        for (int i = 0; i < foldCount; i++) {
            Fold fold = foldManager.getFold(i);
            fold.setCollapsed(true);
        }
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
