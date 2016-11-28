/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.spark.node.sql;

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
import org.knime.base.node.util.KnimeSyntaxTextArea;
import org.knime.base.util.flowvariable.FlowVariableResolver;
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

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;

/**
 * Dialog for the Spark SQL Executor node.
 *
 * @author Sascha Wolke, KNIME.com
 */
class SparkSQLNodeDialog extends NodeDialogPane implements MouseListener {

    private static final Dimension MINIMUM_PANEL_SIZE = new Dimension(200, 10);

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkSQLNodeDialog.class);

    private final SparkSQLSettings m_settings = new SparkSQLSettings();

    private final RSyntaxTextArea m_query;

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
        m_columns.setMinimumSize(MINIMUM_PANEL_SIZE);
        m_columnsPanel = new JScrollPane(m_columns,
            ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
            ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        m_columnsPanel.setBorder(BorderFactory.createTitledBorder(" Column "));

        // Flow variables
        m_variablesModel = new DefaultListModel<>();
        m_variables = new JList<>(m_variablesModel);
        m_variables.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        m_variables.setCellRenderer(new FlowVariableListCellRenderer());
        m_variables.addMouseListener(this);
        m_variables.setMinimumSize(MINIMUM_PANEL_SIZE);
        final JScrollPane variablesPanel = new JScrollPane(m_variables,
            ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
            ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        variablesPanel.setBorder(BorderFactory.createTitledBorder(" Flow Variable "));

        // Functions
        m_functionsError = new JLabel("<html><center>No<br/>function<br/>information<br/>available</center></html>");
        m_functionsModel = new DefaultListModel<>();
        m_functions = new JList<>(m_functionsModel);
        m_functions.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        m_functions.addMouseListener(this);
        m_functions.setMinimumSize(MINIMUM_PANEL_SIZE);
        m_functionsPanel = new JScrollPane(m_functions,
            ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
            ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        m_functionsPanel.setBorder(BorderFactory.createTitledBorder(" Functions "));

        // Split panes
        final JSplitPane rightPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, variablesPanel, m_functionsPanel);
        rightPane.setResizeWeight(0.5);
        final JSplitPane bottomPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, m_columnsPanel, rightPane);
        bottomPane.setResizeWeight(0.66);
        final JSplitPane mainPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, queryPane, bottomPane);
        mainPane.setResizeWeight(0.6);

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
                        completions.add(new ShorthandCompletion(queryCompletition, function, function + "()", "function"));
                    }
                    m_functionsPanel.setViewportView(m_functions);
                    m_functionsPanel.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);

                } catch (KNIMESparkException e) {
                    LOGGER.warn("Unable to fetch SQL functions list: " + e.getMessage(), e);
                    m_functionsPanel.setViewportView(m_functionsError);
                    m_functionsPanel.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_NEVER);
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

        completions.add(new ShorthandCompletion(queryCompletition, SparkSQLJobInput.TABLE_PLACEHOLDER,
            SparkSQLJobInput.TABLE_PLACEHOLDER, " Table placeholder (" + SparkSQLJobInput.TABLE_PLACEHOLDER + ")"));

        queryCompletition.addCompletions(completions);
        final AutoCompletion queryAc = new AutoCompletion(queryCompletition);
        queryAc.install(m_query);
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
                    System.err.println("Fixi");
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
