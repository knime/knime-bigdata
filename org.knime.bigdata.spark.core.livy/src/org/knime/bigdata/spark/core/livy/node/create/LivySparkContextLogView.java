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
 *   Created on 05.07.2018 by bjoern
 */
package org.knime.bigdata.spark.core.livy.node.create;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTextArea;
import javax.swing.ScrollPaneConstants;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingWorker;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.livy.context.LivySparkContext;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

/**
 * Spark log view. Provides a button for fetching the log.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContextLogView extends NodeView<LivySparkContextCreatorNodeModel> implements ActionListener {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(LivySparkContextLogView.class);

    private final JPanel m_mainPanel;

    private final JPanel m_controlPanel;

    private final JButton m_button;

    private final JSpinner m_cacheRows;

    private final JTextArea m_textArea = new JTextArea();

    private final JScrollPane m_textScrollPane = new JScrollPane(m_textArea);

    private final LivySparkContextCreatorNodeModel m_nodeModel;

    private boolean m_isFetching;

    final ExecutionMonitor execMon = new ExecutionMonitor();

    /**
     * Default constructor.
     * 
     * @param model The node model.
     */
    public LivySparkContextLogView(LivySparkContextCreatorNodeModel model) {
        super(model);

        m_nodeModel = model;

        m_mainPanel = new JPanel();
        m_mainPanel.setLayout(new BoxLayout(m_mainPanel, BoxLayout.Y_AXIS));
        m_mainPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        m_button = new JButton("Fetch");
        m_button.addActionListener(this);
        m_cacheRows = new JSpinner(new SpinnerNumberModel(1000, 1, Integer.MAX_VALUE, 100));
        //        m_cacheRows.setMinimumSize(new Dimension(50, 20));
        //        m_cacheRows.setPreferredSize(new Dimension(50, 20));
        m_controlPanel = new JPanel(new FlowLayout());
        m_controlPanel.add(m_button);
        m_controlPanel.add(m_cacheRows);
        m_mainPanel.add(m_controlPanel);

        m_textArea.setEditable(false);
        m_textArea.setWrapStyleWord(true);
        m_textArea.setLineWrap(true);
        m_textArea.setFont(new Font("monospaced", Font.PLAIN, m_textArea.getFont().getSize()));

        m_textScrollPane.setPreferredSize(new Dimension(1200, 600));
        m_textScrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
        m_textScrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
        m_mainPanel.add(m_textScrollPane);

        m_isFetching = false;

        updateUI(false, "");

        setComponent(m_mainPanel);
    }

    /** Reset layout (e.g. remove error messages and drop old data table) */
    public void reset() {
        updateUI(false, "");
    }

    @Override
    public void actionPerformed(final ActionEvent e) {
        if (e.getSource().equals(m_button)) {
            if (!m_isFetching) {
                m_isFetching = true;
                updateUI(true, "Fetching Spark log...");
                loadLog();
            } else {
                m_isFetching = false;
                updateUI(false, "");
                execMon.getProgressMonitor().setExecuteCanceled();

            }
        }
    }

    /** Fetch and show preview table. */
    private void loadLog() {
        execMon.getProgressMonitor().reset();

        final int rows = ((SpinnerNumberModel)m_cacheRows.getModel()).getNumber().intValue();

        final SwingWorker<List<String>, Void> worker = new SwingWorker<List<String>, Void>() {
            @Override
            protected List<String> doInBackground() throws Exception {
                final SparkContextID contextID = m_nodeModel.getSparkContextID();
                final LivySparkContext livyContext =
                    (LivySparkContext)SparkContextManager.<LivySparkContextConfig> getOrCreateSparkContext(contextID);

                return livyContext.getSparkDriverLogs(rows, execMon);
            }

            @Override
            protected void done() {
                m_isFetching = false;
                try {
                    final List<String> logs = super.get();

                    if (logs == null) {
                        updateUI(false, "Error fetching Spark log. For details see KNIME log.");
                    } else {
                        final StringBuilder buf = new StringBuilder();
                        for (String logLine : logs) {
                            buf.append(logLine);
                            buf.append('\n');
                        }
                        updateUI(false, buf.toString());
                    }

                } catch (ExecutionException | InterruptedException ee) {
                    final Throwable cause = ee.getCause();

                    if (cause instanceof CanceledExecutionException) {
                        updateUI(false, "");
                    } else {
                        updateUI(false, "Error while fetching Spark log. Reason: " + ee.getMessage());
                        LOGGER.warn("Error while fetching Spark log. Reason: " + ee.getMessage(), ee);
                    }
                }
            }
        };

        worker.execute();
    }

    /**
     * Default layout with fetch button and (possible empty) table.
     * 
     * @param textToDisplay
     */
    private void updateUI(final boolean isFetching, final String textToDisplay) {
        m_textArea.setText(textToDisplay);
        m_button.setText((isFetching) ? "Cancel" : "Fetch trailing rows");
    }

    @Override
    protected void onClose() {
    }

    @Override
    protected void onOpen() {
        m_isFetching = false;
        updateUI(false, "");
    }

    @Override
    protected void modelChanged() {
        m_isFetching = false;
        updateUI(false, "");
    }
}
