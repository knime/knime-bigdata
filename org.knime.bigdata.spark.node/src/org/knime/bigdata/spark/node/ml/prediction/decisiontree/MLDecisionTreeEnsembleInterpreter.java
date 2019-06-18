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
 *   Created on Jun 18, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.io.RandomAccessFile;
import java.util.List;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingWorker;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLDecisionTreeEnsembleInterpreter implements ModelInterpreter<MLModel> {

    private static final long serialVersionUID = -3345033165871032959L;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(MLDecisionTreeEnsembleInterpreter.class);

    private final MLModelType m_modelType;

    /**
     * @param modelType The unique name of the model to interpret.
     */
    public MLDecisionTreeEnsembleInterpreter(final MLModelType modelType) {
        m_modelType = modelType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return m_modelType.getUniqueName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final MLModel decisionTreeEnsembleModel) {
        final MLDecisionTreeEnsembleMetaData metaData = decisionTreeEnsembleModel.getModelMetaData(MLDecisionTreeEnsembleMetaData.class).get();
        return "Number of trees: " + metaData.getNoOfTrees() + " / Total number of nodes: " + metaData.getTotalNoOfTreeNodes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final MLModel decisionTreeEnsembleModel) {

        return new JComponent[]{getTreePanel(decisionTreeEnsembleModel)};
    }

    private JComponent getTreePanel(final MLModel decisionTreeEnsembleModel) {
        final MLDecisionTreeEnsembleMetaData modelMetaData = decisionTreeEnsembleModel.getModelMetaData(MLDecisionTreeEnsembleMetaData.class).get();
        final List<String> colNames = decisionTreeEnsembleModel.getLearningColumnNames();
        final String classColName = decisionTreeEnsembleModel.getTargetColumnName().get();

        final JComponent component = new JPanel();
        component.setLayout(new BorderLayout());
        component.setBackground(NodeView.COLOR_BACKGROUND);


        final JPanel p = new JPanel(new FlowLayout());
        final SpinnerNumberModel spinnerModel = new SpinnerNumberModel();
        spinnerModel.setValue(2);
        final JSpinner numTreesSelector = new JSpinner(spinnerModel);
        numTreesSelector.setMinimumSize(new Dimension(50, 20));
        numTreesSelector.setPreferredSize(new Dimension(50, 20));
        p.add(numTreesSelector);
        p.add(new JLabel(String.format(" of %d models in total", modelMetaData.getNoOfTrees())));
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
                if (tree > modelMetaData.getNoOfTrees()) {
                    spinnerModel.setValue(modelMetaData.getNoOfTrees());
                }
            }
        });

        numTreesSelector.addChangeListener(new ChangeListener() {
            /** {@inheritDoc} */
            @Override
            public void stateChanged(final ChangeEvent e) {
                final int currTree = ((Number)numTreesSelector.getValue()).intValue();
                treePanel.removeAll();
                treePanel.add(new JLabel("Converting decision tree " + currTree + " ..."), BorderLayout.NORTH);
                treePanel.repaint();
                treePanel.revalidate();
                final SwingWorker<JComponent, Void> worker = new SwingWorker<JComponent, Void>() {
                    /** {@inheritDoc} */
                    @Override
                    protected JComponent doInBackground() throws Exception {
                        final MLDecisionTree tree;
                        try (RandomAccessFile in = new RandomAccessFile(decisionTreeEnsembleModel.getModelInterpreterFile().get().toFile(), "r")) {
                            tree = MLDecisionTreeEnsemble.readTreeAt(in, currTree - 1);
                        }

                        return MLDecisionTreeInterpreter.getTreeView(tree.getRootNode(), colNames, classColName,
                            decisionTreeEnsembleModel);
                    }

                    /** {@inheritDoc} */
                    @Override
                    protected void done() {
                        JComponent dt = null;
                        try {
                            dt = super.get();
                        } catch (Exception e) {
                            LOGGER.warn("Error reading model, reason: " + e.getMessage(), e);
                            final Throwable cause = e.getCause();
                            final String msg;
                            if (cause != null) {
                                msg = cause.getMessage();
                            } else {
                                msg = e.getMessage();
                            }
                            treePanel.removeAll();
                            treePanel.add(new JLabel("Error reading model: " + msg), BorderLayout.NORTH);
                            treePanel.repaint();
                            treePanel.revalidate();
                            return;
                        }
                        if (dt == null) {
                            treePanel.removeAll();
                            treePanel.add(new JLabel("Error reading model. For details see log file."), BorderLayout.NORTH);
                            treePanel.repaint();
                            treePanel.revalidate();
                        } else {
                            final String name = String.format("Decision Tree #%d", currTree);
                            dt.setName(name);
                            treePanel.removeAll();
                            StringBuilder sb = new StringBuilder();
                            sb.append("Showing " + name);
                            double w = getTreeWeight(modelMetaData.getTreeWeights(), currTree - 1);
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

    double getTreeWeight(final double[] treeWeights, final int treeIndex) {
        double sum = 0;
        for (int i = 0; i < treeWeights.length; i++) {
            sum += treeWeights[i];
        }
        return treeWeights[treeIndex] / sum;
    }
}
