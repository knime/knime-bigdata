/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.node.sql;

import java.awt.Dimension;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.ScrollPaneConstants;
import javax.swing.border.EtchedBorder;

import org.fife.ui.autocomplete.AutoCompletion;
import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.DefaultCompletionProvider;
import org.fife.ui.autocomplete.ShorthandCompletion;
import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rtextarea.RTextScrollPane;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.bigdata.spark.core.exception.MissingJobException;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.DataColumnSpecListCellRenderer;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.rsyntaxtextarea.KnimeSyntaxTextArea;

/**
 * Dialog for the Spark SQL Executor node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class SparkSQLNodeDialog extends NodeDialogPane implements MouseListener {

    private static final Dimension MINIMUM_PANEL_SIZE = new Dimension(100, 100);

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkSQLNodeDialog.class);

    private final SparkSQLSettings m_settings = new SparkSQLSettings();

    private final RSyntaxTextArea m_query;
    private AutoCompletion m_autoCompletion;

    private final JScrollPane m_columnsPanel;
    private final DefaultListModel<DataColumnSpec> m_columnsModel;
    private final JList<DataColumnSpec> m_columns;
    private final JLabel m_columnsError;

    private final DefaultListModel<FlowVariable> m_variablesModel;
    private final JList<FlowVariable> m_variables;

    private final JScrollPane m_functionsPanel;
    private final DefaultListModel<String> m_functionsModel;
    private final JList<String> m_functions;
    private final JLabel m_functionsError;

    SparkSQLNodeDialog() {
        // Query
        m_query = createEditor();
        final RTextScrollPane queryPane = new RTextScrollPane(m_query);
        queryPane.setPreferredSize(new Dimension(850, 400));
        queryPane.setFoldIndicatorEnabled(true);
        queryPane.setBorder(BorderFactory.createTitledBorder(" SQL Statement "));

        // Input columns
        m_columnsError = new JLabel("<html><center>No<br/>column<br/>information<br/>available</center></html>");
        m_columnsModel = new DefaultListModel<>();
        m_columns = new JList<>(m_columnsModel);
        m_columns.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        m_columns.setCellRenderer(new DataColumnSpecListCellRenderer());
        m_columns.addMouseListener(this);
        m_columnsPanel = new JScrollPane(m_columns,
            ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
            ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        m_columnsPanel.setBorder(BorderFactory.createTitledBorder(" Column "));
        m_columnsPanel.setMinimumSize(MINIMUM_PANEL_SIZE);

        // Flow variables
        m_variablesModel = new DefaultListModel<>();
        m_variables = new JList<>(m_variablesModel);
        m_variables.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        m_variables.setCellRenderer(new FlowVariableListCellRenderer());
        m_variables.addMouseListener(this);
        final JScrollPane variablesPanel = new JScrollPane(m_variables,
            ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
            ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        variablesPanel.setBorder(BorderFactory.createTitledBorder(" Flow Variable "));
        variablesPanel.setMinimumSize(MINIMUM_PANEL_SIZE);

        // Functions
        m_functionsError = new JLabel("<html><center>No<br/>function<br/>information<br/>available</center></html>");
        m_functionsModel = new DefaultListModel<>();
        m_functions = new JList<>(m_functionsModel);
        m_functions.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        m_functions.addMouseListener(this);
        m_functionsPanel = new JScrollPane(m_functions,
            ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
            ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        m_functionsPanel.setBorder(BorderFactory.createTitledBorder(" Functions "));
        m_functionsPanel.setMinimumSize(MINIMUM_PANEL_SIZE);

        final JSplitPane columnVarPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, m_columnsPanel, variablesPanel);
        columnVarPane.setResizeWeight(0.7);
        columnVarPane.setOneTouchExpandable(true);
        final JSplitPane inputPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, columnVarPane, m_functionsPanel);
        inputPane.setResizeWeight(0.8);
        inputPane.setOneTouchExpandable(true);
        final JSplitPane mainPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, inputPane, queryPane);
        mainPane.setResizeWeight(0.4);
        mainPane.setOneTouchExpandable(true);

        addTab("Query", mainPane, false);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {

        m_settings.loadSettingsFrom(settings);
        m_query.setText(m_settings.getQuery());
        final DefaultCompletionProvider queryCompletition = new DefaultCompletionProvider();
        final List<Completion> completions = new LinkedList<>();

        m_columnsModel.removeAllElements();
        if (specs != null && specs.length > 0 && specs[0] != null) {
            final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec) specs[0];

            // Columns
            for(DataColumnSpec col : spec.getTableSpec()) {
                m_columnsModel.addElement(col);
                final String desc = "Column (" + col.getType().toPrettyString() + ")";
                completions.add(new ShorthandCompletion(queryCompletition, col.getName(), "`" + col.getName() + "`", desc));
            }
            m_columnsPanel.setViewportView(m_columns);
            m_columnsPanel.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);

            // Functions
            if (m_functionsModel.isEmpty()) {
                try {
                    final List<String> functions = SparkSQLNodeModel.getSQLFunctions(spec.getContextID());
                    m_functionsModel.removeAllElements();
                    for (String function : functions) {
                        m_functionsModel.addElement(function);
                        completions.add(new ShorthandCompletion(queryCompletition, function, function + "()", "Function"));
                    }
                    m_functionsPanel.setViewportView(m_functions);
                    m_functionsPanel.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);

                } catch(MissingJobException e) {
                    LOGGER.info("Spark SQL function listing not supported by this Spark version.");
                    m_functionsPanel.setViewportView(m_functionsError);
                    m_functionsPanel.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_NEVER);

                } catch (Exception e) {
                    LOGGER.warn("Unable to fetch SQL functions list: " + e.getMessage(), e);
                    m_functionsPanel.setViewportView(m_functionsError);
                    m_functionsPanel.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_NEVER);
                }
            } else {
                //fill the completion with the elements from the function model
                for (int i = 0; i < m_functionsModel.size(); i++) {
                    final String function = m_functionsModel.get(i);
                    completions.add(new ShorthandCompletion(queryCompletition, function, function + "()", "Function"));
                }
            }

        } else {
            m_columnsPanel.setViewportView(m_columnsError);
            m_columnsPanel.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_NEVER);
            m_functionsPanel.setViewportView(m_functionsError);
            m_functionsPanel.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_NEVER);
        }

        // Variables
        m_variablesModel.removeAllElements();
        for (Map.Entry<String, FlowVariable> e : getAvailableFlowVariables().entrySet()) {
            final FlowVariable var = e.getValue();
            m_variablesModel.addElement(var);
            final String repl = FlowVariableResolver.getPlaceHolderForVariable(var);
            final String desc = "Flow Variable (" + var.getType() + ")";
            completions.add(new ShorthandCompletion(queryCompletition, var.getName(), repl, desc));
        }

        // Table placeholder
        completions.add(new ShorthandCompletion(queryCompletition, SparkSQLJobInput.TABLE_PLACEHOLDER,
            SparkSQLJobInput.TABLE_PLACEHOLDER, " Table placeholder (" + SparkSQLJobInput.TABLE_PLACEHOLDER + ")"));

        queryCompletition.addCompletions(completions);
        if (m_autoCompletion != null) {
            m_autoCompletion.uninstall();
        }
        m_autoCompletion = new AutoCompletion(queryCompletition);
        m_autoCompletion.install(m_query);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setQuery(m_query.getText());
        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
    }

    @Override
    public void mouseClicked(final MouseEvent e) {
        if (e.getClickCount() == 2 && e.getSource() instanceof JList<?>) {
            Object o = ((JList<?>) e.getSource()).getSelectedValue();
            if (o != null) {
                final String value;

                if (o instanceof DataColumnSpec) {
                    value = "`" + ((DataColumnSpec) o).getName() + "`";
                } else if (o instanceof FlowVariable) {
                    value = FlowVariableResolver.getPlaceHolderForVariable((FlowVariable) o);
                } else if (o instanceof String) {
                    value = ((String) o) + "()";
                } else {
                    value = "";
                }

                m_query.replaceSelection(value);
                m_variables.clearSelection();
                m_query.requestFocus();

                if (o instanceof String) {
                    m_query.setCaretPosition(m_query.getCaretPosition() - 1);
                }
            }
        }
    }

    @Override
    public void mousePressed(final MouseEvent e) {}
    @Override
    public void mouseReleased(final MouseEvent e) {}
    @Override
    public void mouseEntered(final MouseEvent e) {}
    @Override
    public void mouseExited(final MouseEvent e) {}

    private static RSyntaxTextArea createEditor(){
        final RSyntaxTextArea editor = new KnimeSyntaxTextArea();
        editor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        editor.setCodeFoldingEnabled(true);
        editor.setAntiAliasingEnabled(true);
        editor.setAutoIndentEnabled(true);
        editor.setFadeCurrentLineHighlight(true);
        editor.setHighlightCurrentLine(true);
        editor.setLineWrap(false);
        editor.setRoundedSelectionEdges(true);
        editor.setBorder(new EtchedBorder());
        editor.setTabSize(4);
        return editor;
    }
}
