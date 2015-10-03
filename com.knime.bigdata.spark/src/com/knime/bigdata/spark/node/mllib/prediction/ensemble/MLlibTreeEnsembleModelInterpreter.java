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
 *   Created on 27.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.ensemble;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingWorker;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.TreeEnsembleModel;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeInterpreter;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author koetter
 * @param <M> {@link TreeEnsembleModel} implementation
 */
public abstract class MLlibTreeEnsembleModelInterpreter<M extends TreeEnsembleModel> extends
    HTMLModelInterpreter<SparkModel<M>> {

    private static final long serialVersionUID = 1L;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(MLlibTreeEnsembleModelInterpreter.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel<M> model) {
        final M treeModel = model.getModel();
        return "Number of trees: " + treeModel.numTrees() + " Total number of nodes: " + treeModel.totalNumNodes();
    }

    @Override
    protected String getHTMLDescription(final SparkModel<M> model) {
        final M treeModel = model.getModel();
        final StringBuilder buf = new StringBuilder();
        buf.append("Number of trees: " + treeModel.numTrees() + " Total number of nodes: " + treeModel.totalNumNodes());
        buf.append("<br>");
        buf.append(" Features: ");
        final List<String> colNames = model.getLearningColumnNames();
        int idx = 0;
        for (final String colName : colNames) {
            if (idx > 0) {
                buf.append(", ");
            }
            buf.append(idx++ + "=" + colName);
        }
        buf.append("<br>");
        String debugString = treeModel.toDebugString();
        //remove first line
        debugString = debugString.replaceFirst(".*\n", "");
        debugString = debugString.replaceAll("\\n", "<br>");
        debugString = debugString.replaceAll("<=", "&le");
        debugString = debugString.replaceAll(">=", "&ge");
        debugString = debugString.replaceAll("\\s", "&nbsp;");
        buf.append(debugString);
        return buf.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final SparkModel<M> aDecisionTreeModel) {

        return new JComponent[]{getTreePanel(aDecisionTreeModel), super.getViews(aDecisionTreeModel)[0]};
    }

    private JComponent getTreePanel(final SparkModel<M> aDecisionTreeModel) {

        final DecisionTreeModel[] treeModel = aDecisionTreeModel.getModel().trees();
        final List<String> colNames = aDecisionTreeModel.getLearningColumnNames();
        final String classColName = aDecisionTreeModel.getClassColumnName();

        final JComponent component = new JPanel();
        component.setLayout(new BorderLayout());
        component.setBackground(NodeView.COLOR_BACKGROUND);

        final int numTrees = treeModel.length;

        final JPanel p = new JPanel(new FlowLayout());
        final JButton b = new JButton("Show tree: ");
        final SpinnerNumberModel spinnerModel = new SpinnerNumberModel();
        spinnerModel.setValue(1);
        final JSpinner numTreesSelector = new JSpinner(spinnerModel);
        numTreesSelector.setMinimumSize(new Dimension(50, 20));
        numTreesSelector.setPreferredSize(new Dimension(50, 20));
        p.add(b);
        p.add(numTreesSelector);
        p.add(new JLabel(" of " + numTrees + "."));
        spinnerModel.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                final int tree = ((Number)numTreesSelector.getValue()).intValue();
                if (tree < 1) {
                    spinnerModel.setValue(1);
                }
                if (tree > numTrees) {
                    spinnerModel.setValue(numTrees);
                }
            }
        });

        component.add(p, BorderLayout.NORTH);
        final JPanel treePanel = new JPanel();
        treePanel.setLayout(new BorderLayout());
        component.add(treePanel, BorderLayout.CENTER);
        component.setName("Decision Tree View");
        b.addActionListener(new ActionListener() {
            /** {@inheritDoc} */
            @Override
            public void actionPerformed(final ActionEvent e) {
                final int tree = ((Number)numTreesSelector.getValue()).intValue();
                treePanel.removeAll();
                treePanel.add(new JLabel("Converting decision tree #" + tree + " ..."), BorderLayout.NORTH);
                treePanel.repaint();
                treePanel.revalidate();
                //TK_TODO: Add job cancel button to the dialog to allow users to stop the fetching job
                final SwingWorker<JComponent, Void> worker = new SwingWorker<JComponent, Void>() {
                    /** {@inheritDoc} */
                    @Override
                    protected JComponent doInBackground() throws Exception {
                        return MLlibDecisionTreeInterpreter.getTreeView(treeModel[tree - 1], colNames, classColName);
                    }

                    /** {@inheritDoc} */
                    @Override
                    protected void done() {
                        JComponent dt = null;
                        try {
                            dt = super.get();
                        } catch (ExecutionException | InterruptedException ee) {
                            LOGGER.warn("Error converting Spark tree model, reason: " + ee.getMessage(), ee);
                            final Throwable cause = ee.getCause();
                            final String msg;
                            if (cause != null) {
                                msg = cause.getMessage();
                            } else {
                                msg = ee.getMessage();
                            }
                            treePanel.removeAll();
                            treePanel.add(new JLabel("Error converting Spark tree model: " + msg), BorderLayout.NORTH);
                            treePanel.repaint();
                            treePanel.revalidate();
                            return;
                        }
                        if (dt == null) {
                            treePanel.removeAll();
                            treePanel.add(new JLabel("Error converting Spark tree model " + tree
                                + ". For details see log file."), BorderLayout.NORTH);
                            treePanel.repaint();
                            treePanel.revalidate();
                        } else {
                            dt.setName("Decision Tree (" + tree + ")");
                            treePanel.removeAll();
                            StringBuilder sb = new StringBuilder();
                            sb.append("Showing Spark tree model number ");
                            sb.append(tree);
                            double w = getTreeWeight(aDecisionTreeModel.getModel(), tree - 1);
                            if (w >= 0) {
                                sb.append(" (weight: ").append(String.format("%3.2f", 100 * w)).append("%)");
                            }
                            sb.append(":");
                            treePanel.add(new JLabel(sb.toString()), BorderLayout.NORTH);
                            treePanel.add(dt, BorderLayout.CENTER);
                            component.setName(dt.getName());
                            component.repaint();
                            component.revalidate();
                        }
                    }
                };
                worker.execute();
            }
        });
        return component;
    }

    double getTreeWeight(final M aDecisionTreeModel, final int aTree) {
        final double[] weights = aDecisionTreeModel.treeWeights();
        double sum = 0;
        for (int i = 0; i < weights.length; i++) {
            sum += weights[i];
        }
        return weights[aTree] / sum;
    }
}