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
 *   Created on Oct 24, 2016 by Sascha Wolke, KNIME.com
 */
package org.knime.bigdata.spark.core.port.data;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.concurrent.ExecutionException;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.JTextArea;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingWorker;

import org.knime.core.data.DataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.BufferedDataTableView;

/**
 * Preview panel with number of rows field, fetch+cancel button and result table.
 *
 * @author Sascha Wolke, KNIME.com
 */
public abstract class SparkDataPreviewPanel extends JPanel implements ActionListener {
    private static final long serialVersionUID = 5010304554610744906L;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkDataPreviewPanel.class);

    final JPanel m_controlPanel;
    final JButton m_loadButton;
    final JSpinner m_cacheRows;

    final JPanel m_fetchingPanel;
    final JLabel m_fetchingLabel;
    final JButton m_cancelButton;

    BufferedDataTableView m_dataTableView = new BufferedDataTableView(null);

    final ExecutionMonitor execMon = new ExecutionMonitor();

    /** Default constructor. */
    public SparkDataPreviewPanel() {
        super(new BorderLayout());
        m_loadButton = new JButton("Cache no. of rows: ");
        m_loadButton.addActionListener(this);
        m_cacheRows = new JSpinner(new SpinnerNumberModel(100, 0, 10000, 10));
        m_cacheRows.setMinimumSize(new Dimension(50, 20));
        m_cacheRows.setPreferredSize(new Dimension(50, 20));
        m_controlPanel = new JPanel(new FlowLayout());
        m_controlPanel.add(m_loadButton);
        m_controlPanel.add(m_cacheRows);

        m_fetchingLabel = new JLabel();
        m_cancelButton = new JButton("Cancel");
        m_cancelButton.addActionListener(this);
        m_fetchingPanel = new JPanel(new FlowLayout());
        m_fetchingPanel.add(m_fetchingLabel);
        m_fetchingPanel.add(m_cancelButton);

        defaultLayout();
    }

    /**
     * Prepare (run job before preview) and return data table used in this preview.
     * @param exec Monitor that might receive cancel by cancel button click.
     * @return Fresh data table
     * @throws Exception on any errors
     */
    protected abstract SparkDataTable prepareDataTable(final ExecutionMonitor exec) throws Exception;

    /** Reset layout (e.g. remove error messages and drop old data table) */
    public void reset() {
        m_dataTableView = new BufferedDataTableView(null);
        defaultLayout();
    }

    @Override
    public void actionPerformed(final ActionEvent e) {
        if (e.getSource().equals(m_loadButton)) {
            loadTable();
        } else if (e.getSource().equals(m_cancelButton)) {
            execMon.getProgressMonitor().setExecuteCanceled();
        }
    }

    /** Fetch and show preview table. */
    private void loadTable() {
        execMon.getProgressMonitor().reset();
        final int rows = ((SpinnerNumberModel) m_cacheRows.getModel()).getNumber().intValue();
        fetchingLayout(rows);

        final SwingWorker<DataTable, Void> worker = new SwingWorker<DataTable, Void>() {
            @Override
            protected DataTable doInBackground() throws Exception {
                return SparkDataTableUtil.getDataTable(execMon, prepareDataTable(execMon), rows);
            }

            @Override
            protected void done() {
                try {
                    DataTable dataTable = super.get();

                    if (dataTable == null) {
                        errorLayout("Error fetching " + rows + " rows from Spark. For details see log file.");
                    } else {
                        m_dataTableView = new BufferedDataTableView(dataTable);
                        defaultLayout();
                    }

                } catch (ExecutionException|InterruptedException ee) {
                    final Throwable cause = ee.getCause();

                    if (cause instanceof CanceledExecutionException) {
                        canceledLayout();

                    } else {
                        LOGGER.warn("Error during fetching data from Spark, reason: " + ee.getMessage(), ee);
                        errorLayout(cause != null ? cause.getMessage() : ee.getMessage());
                    }
                }
            }
        };

        worker.execute();
    }

    /** Default layout with fetch button and (possible empty) table. */
    private void defaultLayout() {
        removeAll();
        add(m_controlPanel, BorderLayout.NORTH);
        add(m_dataTableView, BorderLayout.CENTER);
        repaint();
        revalidate();
    }

    /** Layout with cancel button to show while fetching data. */
    private void fetchingLayout(final int rows) {
        m_fetchingLabel.setText("Fetching " + rows + " rows from Spark...");
        removeAll();
        add(m_fetchingPanel, BorderLayout.NORTH);
        add(new BufferedDataTableView(null), BorderLayout.CENTER);
        repaint();
        revalidate();
    }

    /** Layout after cancel button clicked and job canceled. */
    private void canceledLayout() {
        errorLayout("Execution canceled.");
    }

    /** Layout with error message and fetch button. */
    private void errorLayout(final String message) {
        m_dataTableView = new BufferedDataTableView(null);
        removeAll();
        add(m_controlPanel, BorderLayout.NORTH);
        JTextArea textArea = new JTextArea(message);
        textArea.setWrapStyleWord(true);
        textArea.setLineWrap(true);
        textArea.setBackground(getBackground());
        add(textArea, BorderLayout.CENTER);
        repaint();
        revalidate();
    }
}
