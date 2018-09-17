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
 *
 * History
 *   Created on 28.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.util;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.swing.JButton;

import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.folding.Fold;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.node.scripting.python.PySparkNodeConfig;
import org.knime.bigdata.spark.node.scripting.python.PySparkNodeModel;
import org.knime.bigdata.spark.node.scripting.python.PySparkOutRedirectException;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.generic.SourceCodeConfig;
import org.knime.python2.generic.SourceCodePanel;
import org.knime.python2.generic.VariableNames;

/**
 * Source code panel for pyspark coding.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class PySparkSourceCodePanel extends SourceCodePanel {

    private static final int DEFAULT_VALIADTE_ROW_COUNT = 50;
    private HashMap<String, FlowVariable> m_usedFlowVariables = new HashMap<>();
    private JButton m_validate = new JButton("Validate on Cluster");
    private PortObject[] m_input;
    private SettingsModelInteger m_rowcount = new SettingsModelInteger("RowCount", DEFAULT_VALIADTE_ROW_COUNT);

    /**
     * Creates a PySparkSourceCode Panel for the given variables
     *
     * @param variableNames the variables names
     * @param pySparkDocument the document to display
     */
    public PySparkSourceCodePanel(final VariableNames variableNames, final PySparkDocument pySparkDocument){
        super(SyntaxConstants.SYNTAX_STYLE_PYTHON, variableNames);
        getEditor().setDocument(pySparkDocument);
        NumberFormat format = NumberFormat.getIntegerInstance();
        format.setParseIntegerOnly(true);
        format.setMaximumIntegerDigits(4);
        format.setMinimumIntegerDigits(1);

        DialogComponentNumberEdit rowCount =
                new DialogComponentNumberEdit(m_rowcount, "Number of rows to validate on", 5);
        m_editorButtons.removeAll();
        m_editorButtons.add(m_validate);
        m_editorButtons.add(rowCount.getComponentPanel());
        m_validate.addActionListener(new ActionListener() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void actionPerformed(final ActionEvent e) {
                runExec("");
            }
        });
        if(m_input == null) {
            setInteractive(false);
            m_validate.setEnabled(false);
        }
        showWorkspacePanel(false);
    }

    private static final long serialVersionUID = 1821218187292351246L;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runExec(final String sourceCode) {
        int outCount = getVariableNames().getOutputObjects().length;
        PySparkDocument doc = (PySparkDocument)getEditor().getDocument();
        PortObject[] outObj = {};
        if(m_input == null) {
            errorToConsole("No input data available");
        }else {
            try {
                outObj = PySparkNodeModel.executeScript(m_input, m_input.length, outCount, doc, null,
                    m_rowcount.getIntValue());
            } catch (PySparkOutRedirectException e) {
                messageToConsole(e.getMessage());
             }catch (Exception e) {
               errorToConsole(e.getMessage());
            }
        }
        for(PortObject ob : outObj) {
            SparkDataPortObject sparkObj = (SparkDataPortObject)ob;

            messageToConsole(sparkObj.getTableSpec().toString() + "\n");
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void updateVariables() {
        //Nothing to do here
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runReset() {
        //Nothing to do here
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Completion> getCompletionsFor(final CompletionProvider provider, final String sourceCode,
        final int line, final int column) {

        return new ArrayList<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String createVariableAccessString(final String variable, final String field) {
        if (variable.equalsIgnoreCase(getVariableNames().getFlowVariables())) {
            Iterator<FlowVariable> flowVariablesIter = getFlowVariables().iterator();
            while (flowVariablesIter.hasNext()) {
                FlowVariable flowV = flowVariablesIter.next();
                if (flowV.getName().equalsIgnoreCase(field)) {
                    m_usedFlowVariables.put(field, flowV);
                    break;
                }
            }
            writeFlowVariables();
            return "v_" + field;
        } else {

            return "'" + field + "'";
        }
    }

    @Override
    public void updateFlowVariables(final FlowVariable[] flowVariables) {
        for (FlowVariable flowV : flowVariables) {
            if (m_usedFlowVariables.containsKey(flowV.getName())) {
                m_usedFlowVariables.put(flowV.getName(), flowV);
            }
        }
        writeFlowVariables();
        super.updateFlowVariables(flowVariables);
    }

    private void writeFlowVariables() {
        final FlowVariable[] variables =
            m_usedFlowVariables.values().toArray(new FlowVariable[m_usedFlowVariables.size()]);
        if(variables.length > 0) {
            final PySparkDocument doc = (PySparkDocument)getEditor().getDocument();
            doc.writeFlowVariables(variables);
            getEditor().setDocument(doc);
        }
    }

    @Override
    public void saveSettingsTo(final SourceCodeConfig config) throws InvalidSettingsException {
        PySparkNodeConfig pySparkConfig = (PySparkNodeConfig)config;
        pySparkConfig.setDoc((PySparkDocument)getEditor().getDocument());
    }

    @Override
    public void loadSettingsFrom(final SourceCodeConfig config, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        getEditor().setDocument(((PySparkNodeConfig)config).getDoc());
        int foldCount = getEditor().getFoldManager().getFoldCount();
        for (int i = 0; i < foldCount; i++) {
            Fold fold = getEditor().getFoldManager().getFold(i);
            fold.setCollapsed(true);
        }
        final List<DataTableSpec> tableSpecs = new ArrayList<>();
        for (final PortObjectSpec spec : specs) {
            if (spec instanceof SparkDataPortObjectSpec) {
                tableSpecs.add(((SparkDataPortObjectSpec)spec).getTableSpec());
            }
        }
        updateSpec(tableSpecs.toArray(new DataTableSpec[tableSpecs.size()]));
    }

    /**
     * Updates the data for the validation
     * @param input the input data
     */
    public void updatePortObjects(final PortObject[] input) {
        boolean active = (input != null);
        m_input = input != null ? input.clone() : null;
        setInteractive(active);
        m_validate.setEnabled(active);

    }

}
