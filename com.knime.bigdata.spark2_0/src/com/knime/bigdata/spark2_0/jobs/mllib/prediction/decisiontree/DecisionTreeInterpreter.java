/* ------------------------------------------------------------------
O * This source code, its documentation and all appendant files
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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark2_0.jobs.mllib.prediction.decisiontree;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingWorker;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.tree.Node;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import com.knime.bigdata.spark.core.port.model.ModelInterpreter;
import com.knime.bigdata.spark.core.port.model.SparkModel;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeNodeModel;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.MLlibDecisionTreeGraphPanel;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.MLlibDecisionTreeGraphView;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.TreeNode;

/**
 * @author Ole Ostergaard
 */
public class DecisionTreeInterpreter implements ModelInterpreter {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DecisionTreeInterpreter.class);

    private static final long serialVersionUID = 1L;

    private static volatile DecisionTreeInterpreter instance;

    private DecisionTreeInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static DecisionTreeInterpreter getInstance() {
        if (instance == null) {
            synchronized (DecisionTreeInterpreter.class) {
                if (instance == null) {
                    instance = new DecisionTreeInterpreter();
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return MLlibDecisionTreeNodeModel.MODEL_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel model) {
        if (model.getModel() instanceof DecisionTreeModel) {
            final DecisionTreeModel treeModel = (DecisionTreeModel)model.getModel();
            return "Tree depth: " + treeModel.depth() + " Number of nodes: " + treeModel.numNodes();
        } else if (model.getModel() instanceof PipelineModel) {
            final PipelineModel treeModel = (PipelineModel)model.getModel();
            for (Transformer stage : treeModel.stages()) {
                if (stage instanceof DecisionTreeClassificationModel) {
                     Node tmpRootNode = ((DecisionTreeClassificationModel)stage).rootNode();
                     return "Tree depth: " + tmpRootNode.subtreeDepth() + " Number of nodes: " + tmpRootNode.numDescendants();
                }
            }
            return "not sure";
        } else {
            throw new RuntimeException("Unsupported model type: " + model.getModel().getClass().getName());
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final SparkModel aDecisionTreeModel) {
        return new JComponent[]{getTreePanel(aDecisionTreeModel)};
    }

    /**
     * converts the given tree model into PMML and packs it into a JComponent
     *
     * @param rootNode
     * @param aColNames
     * @param aClassColName
     * @param metaData
     * @return displayable component
     */
    public static JComponent getTreeView(final TreeNode rootNode, final List<String> aColNames,
        final String aClassColName, final ColumnBasedValueMapping metaData) {
        final Map<Integer, String> features = new HashMap<>();
        int ctr = 0;
        for (String col : aColNames) {
            features.put(ctr++, col);
        }
        features.put(ctr, aClassColName);

        final MLlibDecisionTreeGraphView graph = new MLlibDecisionTreeGraphView(rootNode, features, metaData);
        final JComponent view = new MLlibDecisionTreeGraphPanel(new MLlibDecisionTreeNodeModel(), graph);
        view.setName("MLLib TreeView");
        return view;
    }

    private JComponent getTreePanel(final SparkModel aDecisionTreeModel) {

        final ColumnBasedValueMapping metaData = (ColumnBasedValueMapping)aDecisionTreeModel.getMetaData();
        final List<String> colNames = aDecisionTreeModel.getLearningColumnNames();
        final String classColName = aDecisionTreeModel.getClassColumnName();

        final JComponent component = new JPanel();
        component.setLayout(new BorderLayout());
        component.setBackground(NodeView.COLOR_BACKGROUND);

        final JPanel p = new JPanel(new FlowLayout());

        component.add(p, BorderLayout.NORTH);
        final JPanel treePanel = new JPanel();
        treePanel.setLayout(new BorderLayout());
        component.add(treePanel, BorderLayout.CENTER);
        component.setName("Decision Tree View");
        treePanel.removeAll();
        treePanel.add(new JLabel("Converting decision tree ..."), BorderLayout.NORTH);
        treePanel.repaint();
        treePanel.revalidate();
        final TreeNode rootNode;
        if (aDecisionTreeModel.getModel() instanceof PipelineModel) {
            final PipelineModel treeModel = (PipelineModel)aDecisionTreeModel.getModel();
            TreeNode tmpRootNode = null;
            for (Transformer stage : treeModel.stages()) {
                if (stage instanceof DecisionTreeClassificationModel) {
                    tmpRootNode = getRootNode(((DecisionTreeClassificationModel)stage).rootNode());
                }
            }
            if (tmpRootNode == null) {
                throw new RuntimeException("No root node found in pipeline model.");
            }
            rootNode = tmpRootNode;
        } else {
            final DecisionTreeModel treeModel = (DecisionTreeModel)aDecisionTreeModel.getModel();
            rootNode = getRootNode(treeModel);
        }

        //TK_TODO: Add job cancel button to the dialog to allow users to stop the fetching job
        final SwingWorker<JComponent, Void> worker = new SwingWorker<JComponent, Void>() {
            /** {@inheritDoc} */
            @Override
            protected JComponent doInBackground() throws Exception {
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
                    treePanel.add(new JLabel("Error converting Spark tree model. For details see log file."),
                        BorderLayout.NORTH);
                    treePanel.repaint();
                    treePanel.revalidate();
                } else {
                    treePanel.removeAll();
                    treePanel.add(dt, BorderLayout.CENTER);
                    component.setName(dt.getName());
                    component.repaint();
                    component.revalidate();
                }
            }
        };
        worker.execute();
        return component;
    }

    /**
     * @param aRootNode the root node of a DecisionTreeClassificationModel
     * @return
     */
    private TreeNode getRootNode(final Node aRootNode) {
        final TreeNode rootNode = new com.knime.bigdata.spark2_0.jobs.ml.prediction.decisiontree.TreeNode2_0(aRootNode);
        return rootNode;
    }

    /**
     * @param treeModel the {@link DecisionTreeModel}
     * @return the {@link TreeNode} that wraps the root node
     */
    protected TreeNode getRootNode(final DecisionTreeModel treeModel) {
        final TreeNode rootNode = new TreeNode2_0(treeModel.topNode());
        return rootNode;
    }
}
