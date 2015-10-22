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
 *
 * History
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package com.knime.bigdata.spark.node.scorer.accuracy;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.text.NumberFormat;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

import org.knime.base.node.mine.scorer.accuracy.ConfusionTableModel;
import org.knime.core.node.NodeView;

/**
 * This view displays the scoring results. It needs to be hooked up with a
 * scoring model.
 *
 * TODO: code duplicate of {@link AccuracyScorerNodeView}, with removed hiliting functionality
 *
 * @author Christoph Sieb, University of Konstanz
 */
final class SparkAccuracyScorerNodeView extends NodeView<SparkAccuracyScorerNodeModel> {
    /*
     * Components displaying the scorer table, number of correct/wrong
     * classified patterns, and the error percentage number.
     */
    private final JTable m_tableView;

    private JLabel m_correct;

    private JLabel m_wrong;

    private JLabel m_error;

    private JLabel m_accuracy;

    private final JLabel m_cohenKappa;

    /**
     * Creates a new SparkAccuracyScorerNodeView displaying the table with the score.
     *
     * The view consists of the table with the example data and the appropriate
     * scoring in the upper part and the summary of correct and wrong classified
     * examples in the lower part.
     *
     * @param nodeModel
     *            the underlying <code>NodeModel</code>
     */
    public SparkAccuracyScorerNodeView(final SparkAccuracyScorerNodeModel nodeModel) {
        super(nodeModel);

        m_tableView = new JTable();
        m_tableView.setRowSelectionAllowed(false);
        m_tableView.setCellSelectionEnabled(true);
        m_tableView.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        JScrollPane scrollPane = new JScrollPane(m_tableView);

        JPanel summary = new JPanel(new GridLayout(3, 2));

        JPanel labelPanel = new JPanel(new FlowLayout());
        labelPanel.add(new JLabel("Correct classified:"));
        m_correct = new JLabel("n/a");
        labelPanel.add(m_correct);
        summary.add(labelPanel);

        labelPanel = new JPanel(new FlowLayout());
        labelPanel.add(new JLabel("Wrong classified:"));
        m_wrong = new JLabel("n/a");
        labelPanel.add(m_wrong);
        summary.add(labelPanel);

        labelPanel = new JPanel(new FlowLayout());
        labelPanel.add(new JLabel("Accuracy:"));
        m_accuracy = new JLabel("n/a");
        labelPanel.add(m_accuracy);
        labelPanel.add(new JLabel("%"));
        summary.add(labelPanel);

        labelPanel = new JPanel(new FlowLayout());
        labelPanel.add(new JLabel("Error:"));
        m_error = new JLabel("n/a");
        labelPanel.add(m_error);
        labelPanel.add(new JLabel("%"));
        summary.add(labelPanel);

        labelPanel = new JPanel(new FlowLayout());
        labelPanel.add(new JLabel("Cohen's kappa (\u03BA)"));
        m_cohenKappa = new JLabel("n/a");
        labelPanel.add(m_cohenKappa);
        summary.add(labelPanel);


        JPanel outerPanel = new JPanel(new BorderLayout());
        outerPanel.add(scrollPane, BorderLayout.CENTER);
        outerPanel.add(summary, BorderLayout.SOUTH);

        setComponent(outerPanel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void modelChanged() {
        SparkAccuracyScorerViewData viewData = getNodeModel().getViewData();
        if (viewData == null) {
            // model is not executed yet, or was reset.
            m_correct.setText(" n/a ");
            m_wrong.setText(" n/a ");
            m_error.setText(" n/a ");
            m_error.setToolTipText(null);
            m_accuracy.setText(" n/a ");
            m_accuracy.setToolTipText(null);
            m_cohenKappa.setText(" n/a ");
            // m_precision.setText(" n/a ");
            m_tableView.setModel(new DefaultTableModel());
            return;
        }

        String rowHeaderDescription = viewData.getFirstCompareColumn();
        String columnHeaderDescription = viewData.getSecondCompareColumn();

        ConfusionTableModel dataModel = new ConfusionTableModel(viewData.getScorerCount(),
            viewData.getTargetValues(), rowHeaderDescription, columnHeaderDescription);

        m_tableView.setModel(dataModel);

        NumberFormat nf = NumberFormat.getInstance();
        m_correct.setText(nf.format(viewData.getCorrectCount()));
        m_wrong.setText(nf.format(viewData.getFalseCount()));
        double error = 100.0 * viewData.getError();
        m_error.setText(nf.format(error));
        m_error.setToolTipText("Error: " + error + " %");
        double accurarcy = 100.0 * viewData.getAccuracy();
        m_accuracy.setText(nf.format(accurarcy));
        m_accuracy.setToolTipText("Accuracy: " + accurarcy + " %");
        double cohenKappa = viewData.getCohenKappa();
        m_cohenKappa.setText(nf.format(cohenKappa));
        m_cohenKappa.setToolTipText("Cohen's \u03BA: " + cohenKappa);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onClose() {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onOpen() {
        // do nothing
    }
}
