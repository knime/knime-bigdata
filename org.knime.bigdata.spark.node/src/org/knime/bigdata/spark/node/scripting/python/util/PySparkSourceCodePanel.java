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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.swing.JButton;
import javax.swing.JOptionPane;

import org.fife.ui.autocomplete.BasicCompletion;
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
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.PythonKernelTester;
import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.generic.SourceCodeConfig;
import org.knime.python2.generic.SourceCodePanel;
import org.knime.python2.generic.VariableNames;
import org.knime.python2.kernel.PythonKernelManager;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.messaging.PythonKernelResponseHandler;

/**
 * Source code panel for PySpark coding.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class PySparkSourceCodePanel extends SourceCodePanel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PySparkSourceCodePanel.class);

    private static final long serialVersionUID = 1821218187292351246L;

    private static final String VALIDATE_ON_CLUSTER = "Validate on Cluster";

    private static final int DEFAULT_VALIADTE_ROW_COUNT = 50;

    private final Map<String, String> m_usedFlowVariableNames = new HashMap<>();

    private final JButton m_validate = new JButton(VALIDATE_ON_CLUSTER);

    private final JButton m_cancel = new JButton("Cancel");

    private PortObject[] m_input;

    private final SettingsModelInteger m_rowcount = new SettingsModelInteger("RowCount", DEFAULT_VALIADTE_ROW_COUNT);

    private PythonKernelOptions m_kernelOptions = new PythonKernelOptions();

    private final Lock m_lock = new ReentrantLock();

    private ExecutionMonitor m_exec;

    private PythonKernelManager m_kernelManager;

    private String m_warningMessage = "";

    /**
     * Creates a PySparkSourceCode Panel for the given variables
     *
     * @param variableNames the variables names
     * @param pySparkDocument the document to display
     */
    public PySparkSourceCodePanel(final VariableNames variableNames, final PySparkDocument pySparkDocument) {
        super(SyntaxConstants.SYNTAX_STYLE_PYTHON, variableNames);
        getEditor().setDocument(pySparkDocument);
        final NumberFormat format = NumberFormat.getIntegerInstance();
        format.setParseIntegerOnly(true);
        format.setMaximumIntegerDigits(4);
        format.setMinimumIntegerDigits(1);
        final DialogComponentNumberEdit rowCount =
            new DialogComponentNumberEdit(m_rowcount, "Number of rows to validate on", 5);
        m_editorButtons.removeAll();
        m_editorButtons.add(m_cancel);
        m_cancel.setVisible(false);
        m_editorButtons.add(m_validate);
        m_editorButtons.add(rowCount.getComponentPanel());
        m_validate.addActionListener(new ActionListener() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void actionPerformed(final ActionEvent e) {
                m_validate.setEnabled(false);
                m_validate.setText("Executing job on cluster");
                m_cancel.setVisible(true);
                m_editorButtons.revalidate();

                final Thread t = new Thread(new Executer());
                t.start();
            }
        });

        m_cancel.addActionListener(new ActionListener() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void actionPerformed(final ActionEvent e) {

                m_exec.getProgressMonitor().setExecuteCanceled();

            }
        });

        if (m_input == null) {
            setInteractive(false);
            m_validate.setEnabled(false);
        }
        showWorkspacePanel(false);

        collapsFolds();
    }

    private class Executer implements Runnable {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            final int outCount = getVariableNames().getOutputObjects().length;
            final PySparkDocument doc = (PySparkDocument)getEditor().getDocument();
            PortObject[] outObj = {};
            if (m_input == null) {
                errorToConsole("No input data available");
            } else {

                try {
                    m_exec = new ExecutionMonitor();
                    outObj = PySparkNodeModel.executeScript(m_input, m_input.length, outCount, doc, m_exec,
                        m_rowcount.getIntValue());
                } catch (final PySparkOutRedirectException e) {
                    messageToConsole(e.getMessage());
                } catch (final Exception e) {
                    String message = e.getMessage();
                    try {
                        m_exec.checkCanceled();
                    } catch (final CanceledExecutionException ce) {
                        message = "Execution Canceled.";
                    }
                    errorToConsole(message);
                }
            }
            for (final PortObject ob : outObj) {
                final SparkDataPortObject sparkObj = (SparkDataPortObject)ob;

                messageToConsole(sparkObj.getTableSpec().toString() + "\n");
            }
            runFinished();
        }

        private void runFinished() {
            m_validate.setEnabled(true);
            m_validate.setText(VALIDATE_ON_CLUSTER);
            m_cancel.setVisible(false);
            m_editorButtons.revalidate();
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() {
        super.open();
        if (m_kernelManager == null) {
            setStatusMessage("Starting local python for autocompletion...");
            startKernelManagerAsync();
        }
        if(!m_warningMessage.isEmpty() ) {
            showWarning();
        }
    }



    private void showWarning() {
        JOptionPane.showMessageDialog(null, m_warningMessage, "Flow variable warning", JOptionPane.WARNING_MESSAGE);
        errorToConsole(m_warningMessage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        super.close();
        if (m_kernelManager != null) {
            m_kernelManager.close();
        }
        m_kernelManager = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runExec(final String sourceCode) {

        final int outCount = getVariableNames().getOutputObjects().length;
        final PySparkDocument doc = (PySparkDocument)getEditor().getDocument();
        PortObject[] outObj = {};
        if (m_input == null) {
            errorToConsole("No input data available");
        } else {

            try {
                m_exec = new ExecutionMonitor();
                outObj = PySparkNodeModel.executeScript(m_input, m_input.length, outCount, doc, m_exec,
                    m_rowcount.getIntValue());
            } catch (final PySparkOutRedirectException e) {
                messageToConsole(e.getMessage());
            } catch (final Exception e) {
                errorToConsole(e.getMessage());
            }
        }
        printTableSpecsToConsol(outObj);

    }

    private void printTableSpecsToConsol(final PortObject[] outObj) {
        for (final PortObject ob : outObj) {
            final SparkDataPortObject sparkObj = (SparkDataPortObject)ob;
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
        // This list will be filled from another thread
        final List<Completion> completions = new ArrayList<>();
        // If no kernel is running we will simply return the empty list of
        // completions

        if (m_kernelManager != null) {

            final Exchanger<List<Completion>> exchanger = new Exchanger<>();
            final List<Completion> completionsList = new ArrayList<>();

            m_kernelManager.autoComplete(sourceCode, line, column,
                new PythonKernelResponseHandler<List<Map<String, String>>>() {

                    @Override
                    public void handleResponse(final List<Map<String, String>> response, final Exception exception) {
                        if (exception == null) {
                            for (final Map<String, String> completion : response) {
                                String name = completion.get("name");
                                final String type = completion.get("type");
                                String doc = completion.get("doc").trim();
                                if (type.equals("function")) {
                                    name += "()";
                                }
                                doc = "<html><body><pre>" + doc.replace("\n", "<br />") + "</pre></body></html>";
                                completionsList.add(new BasicCompletion(provider, name, type, doc));
                            }
                            try {
                                exchanger.exchange(completionsList, 2, TimeUnit.SECONDS);
                            } catch (InterruptedException | TimeoutException e) {
                                // Nothing to do.
                            }
                        }
                    }
                });

            try {
                return exchanger.exchange(completions, 2, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                // Nothing to do, we will just return an empty list.
            }
        }

        return completions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String createVariableAccessString(final String variable, final String field) {

        if (variable.equalsIgnoreCase(getVariableNames().getFlowVariables())) {
            final String escapedName = VariableNameUtil.escapeName(field);
            if (!m_usedFlowVariableNames.containsKey(escapedName)) {
                m_usedFlowVariableNames.put(escapedName, field);
            } else {
                if (!m_usedFlowVariableNames.get(escapedName).equals(field)) {
                    errorToConsole(String.format(
                        "The escaped name %s for the flowvariable %s already exists. Please rename the variable.",
                        escapedName, field));
                    return "";
                }
            }
            writeFlowVariables();
            return PySparkDocument.FLOW_VARIABLE_START + VariableNameUtil.escapeName(field)
                + PySparkDocument.FLOW_VARIABLE_END;
        } else {
            return String.format("%s['%s']", variable, field);
        }
    }

    @Override
    public void updateFlowVariables(final FlowVariable[] flowVariables) {
        super.updateFlowVariables(flowVariables);
        writeFlowVariables();
    }

    private void writeFlowVariables() {

        final FlowVariable[] usedFlowVariables = getUsedFlowVariables();

        if (usedFlowVariables.length != 0) {
            final PySparkDocument doc = (PySparkDocument)getEditor().getDocument();
            doc.writeFlowVariables(usedFlowVariables);
            getEditor().setDocument(doc);
        }
    }

    private FlowVariable[] getUsedFlowVariables() {
        final List<FlowVariable> currentVariables = getFlowVariables();

        final List<Object> variables = currentVariables.stream().filter(new Predicate<FlowVariable>() {

            @Override
            public boolean test(final FlowVariable f) {
                return m_usedFlowVariableNames.values().contains(f.getName());
            }

        }).collect(Collectors.toList());
        return variables.toArray(new FlowVariable[variables.size()]);
    }

    @Override
    public void saveSettingsTo(final SourceCodeConfig config) throws InvalidSettingsException {
        final PySparkNodeConfig pySparkConfig = (PySparkNodeConfig)config;
        pySparkConfig.setDoc((PySparkDocument)getEditor().getDocument());
        pySparkConfig.setUsedFlowVariablesNames(
            m_usedFlowVariableNames.values().toArray(new String[m_usedFlowVariableNames.size()]));
    }

    @Override
    public void loadSettingsFrom(final SourceCodeConfig config, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        getEditor().setDocument(((PySparkNodeConfig)config).getDoc());
        collapsFolds();
        final List<DataTableSpec> tableSpecs = getTableSpecList(specs);
        updateSpec(tableSpecs.toArray(new DataTableSpec[tableSpecs.size()]));

        final String[] usedVariables = ((PySparkNodeConfig)config).getUsedFlowVariablesNames();
        if (usedVariables.length != 0) {
            m_usedFlowVariableNames.clear();
            for (final String var : usedVariables) {
                m_usedFlowVariableNames.put(VariableNameUtil.escapeName(var), var);
            }

            writeFlowVariables();
        }
    }

    private void collapsFolds() {
        final int foldCount = getEditor().getFoldManager().getFoldCount();
        for (int i = 0; i < foldCount; i++) {
            final Fold fold = getEditor().getFoldManager().getFold(i);
            fold.setCollapsed(true);
        }
    }

    private static List<DataTableSpec> getTableSpecList(final PortObjectSpec[] specs) {
        final List<DataTableSpec> tableSpecs = new ArrayList<>();
        for (final PortObjectSpec spec : specs) {
            if (spec instanceof SparkDataPortObjectSpec) {
                tableSpecs.add(((SparkDataPortObjectSpec)spec).getTableSpec());
            }
        }
        return tableSpecs;
    }

    /**
     * Updates the data for the validation
     *
     * @param input the input data
     */
    public void updatePortObjects(final PortObject[] input) {

        final boolean active = (input != null);
        m_input = input != null ? input.clone() : null;
        setInteractive(active);
        m_validate.setEnabled(active);

    }

    private void startKernelManagerAsync() {
        // Start python in another thread, this might take a few seconds
        ThreadUtils.threadWithContext(new Runnable() {

            @Override
            public void run() {
                setInteractive(false);
                setRunning(false);
                // Test if local python installation is capable of running
                // the kernel
                // This will return immediately if the test result was
                // positive before
                final PythonKernelTestResult result = m_kernelOptions.getUsePython3()
                    ? PythonKernelTester.testPython3Installation(m_kernelOptions.getPython3Command(),
                        m_kernelOptions.getAdditionalRequiredModules(), false)
                    : PythonKernelTester.testPython2Installation(m_kernelOptions.getPython2Command(),
                        m_kernelOptions.getAdditionalRequiredModules(), false);
                // Display result message (this might just be a warning
                // about missing optional modules)
                if (result.hasError()) {
                    errorToConsole(result.getErrorLog() + "\nPlease refer to the KNIME log file for more details.");
                    setStopped();
                    setStatusMessage("Error during python start.");
                } else {
                    // Start kernel manager which will start the actual kernel
                    m_lock.lock();
                    try {
                        setStatusMessage("Starting python...");
                        try {
                            m_kernelManager = new PythonKernelManager(m_kernelOptions);
                        } catch (final Exception ex) {
                            LOGGER.debug("Could not start python kernel.", ex);
                            errorToConsole("Could not start python kernel. Please refer to the KNIME console"
                                + " and log file for details.");
                            setStopped();
                            setStatusMessage("Error during python start");
                        }
                        if (m_kernelManager != null) {
                            setStatusMessage("Python successfully started");
                        }
                    } finally {
                        m_lock.unlock();
                    }
                    if (m_kernelManager != null) {
                        setInteractive(true);

                    }
                }
            }
        }).start();
    }

    /**
     * @param pySparkPath the pySpark path to set
     */
    public void setPySparkPath(final String pySparkPath) {
        m_kernelOptions = m_kernelOptions.forExternalCustomPath(pySparkPath);
    }

    /**
     * @param warningMessage the warningMessage to set
     */
    public void setWarningMessage(final String warningMessage) {
        m_warningMessage = warningMessage;
    }

}
