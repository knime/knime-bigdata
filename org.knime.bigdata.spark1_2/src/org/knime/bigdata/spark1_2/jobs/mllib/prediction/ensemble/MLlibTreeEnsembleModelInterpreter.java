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
 *   Created on 27.09.2015 by koetter
 */
package org.knime.bigdata.spark1_2.jobs.mllib.prediction.ensemble;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingWorker;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.TreeEnsembleModel;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.bigdata.spark.core.port.model.MLlibModel;
import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.TreeNode;
import org.knime.bigdata.spark1_2.jobs.mllib.prediction.decisiontree.DecisionTreeInterpreter;
import org.knime.bigdata.spark1_2.jobs.mllib.prediction.decisiontree.TreeNode1_2;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

import scala.Enumeration.Value;

/**
 *
 * @author koetter
 * @author Ole Ostergaard
 * @param <M> {@link TreeEnsembleModel} implementation
 */
public abstract class MLlibTreeEnsembleModelInterpreter<M extends TreeEnsembleModel> implements
    ModelInterpreter<MLlibModel> {

    private static final long serialVersionUID = 1L;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(MLlibTreeEnsembleModelInterpreter.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final MLlibModel model) {
        @SuppressWarnings("unchecked")
        final M treeModel = (M)model.getModel();
        return "Number of trees: " + treeModel.numTrees() + " / Total number of nodes: " + treeModel.totalNumNodes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final MLlibModel decisionTreeModel) {

        return new JComponent[]{getTreePanel(decisionTreeModel)};
    }

    private JComponent getTreePanel(final MLlibModel decisionTreeModel) {
        @SuppressWarnings("unchecked")
        final M ensembleModel = (M)decisionTreeModel.getModel();
        Value algo = ensembleModel.algo();
        final boolean isClassification = Algo.Classification().equals(algo) ? true : false;
        final DecisionTreeModel[] treeModel = ensembleModel.trees();
        final List<String> colNames = decisionTreeModel.getLearningColumnNames();
        final ColumnBasedValueMapping metaData = (ColumnBasedValueMapping)decisionTreeModel.getMetaData();
        final String classColName = decisionTreeModel.getTargetColumnName();

        final JComponent component = new JPanel();
        component.setLayout(new BorderLayout());
        component.setBackground(NodeView.COLOR_BACKGROUND);

        final int numTrees = treeModel.length;

        final JPanel p = new JPanel(new FlowLayout());
        final SpinnerNumberModel spinnerModel = new SpinnerNumberModel();
        spinnerModel.setValue(2);
        final JSpinner numTreesSelector = new JSpinner(spinnerModel);
        numTreesSelector.setMinimumSize(new Dimension(50, 20));
        numTreesSelector.setPreferredSize(new Dimension(50, 20));
        p.add(numTreesSelector);
        p.add(new JLabel(" of " + numTrees + "."));
        component.add(p, BorderLayout.NORTH);
        final JPanel treePanel = new JPanel();
        treePanel.setLayout(new BorderLayout());
        component.add(treePanel, BorderLayout.CENTER);
        component.setName("Decision Tree View");
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

        numTreesSelector.addChangeListener(new ChangeListener() {
            /** {@inheritDoc} */
            @Override
            public void stateChanged(final ChangeEvent e) {
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
                        final DecisionTreeModel model = treeModel[tree - 1];
                        final TreeNode rootNode = getRootNode(model, isClassification);
                        return DecisionTreeInterpreter.getTreeView(rootNode, colNames, classColName, metaData);
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
                            double w = getTreeWeight(ensembleModel, tree - 1);
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
        numTreesSelector.setValue(1);
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

    /**
     * @param treeModel the {@link DecisionTreeModel}
     * @param isClassification
     * @return the {@link TreeNode} that wraps the root node
     */
    protected TreeNode getRootNode(final DecisionTreeModel treeModel, final boolean isClassification) {
        final TreeNode rootNode = new TreeNode1_2(treeModel.topNode());
        return rootNode;
    }
}