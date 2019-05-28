/* ------------------------------------------------------------------
O * This source code, its documentation and all appendant files
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
 *   Created on 21.07.2015 by koetter
 */
package org.knime.bigdata.spark2_4.jobs.mllib.prediction.decisiontree;

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

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.bigdata.spark.core.port.model.MLlibModel;
import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeNodeModel;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.MLlibDecisionTreeGraphPanel;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.MLlibDecisionTreeGraphView;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.TreeNode;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

/**
 * @author Ole Ostergaard
 */
public class DecisionTreeInterpreter implements ModelInterpreter<MLlibModel> {

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
    public String getSummary(final MLlibModel model) {
        final DecisionTreeModel treeModel = (DecisionTreeModel)model.getModel();
        return String.format("Tree depth: %d / Number of nodes: %d", treeModel.depth(), treeModel.numNodes());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final MLlibModel aDecisionTreeModel) {
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
        int ctr =0;
        for (String col : aColNames) {
            features.put(ctr++, col);
        }
        features.put(ctr, aClassColName);

        final MLlibDecisionTreeGraphView graph = new MLlibDecisionTreeGraphView(rootNode, features, metaData);
        final JComponent view = new MLlibDecisionTreeGraphPanel(new MLlibDecisionTreeNodeModel(), graph);
        view.setName("MLLib TreeView");
        return view;
    }

    private JComponent getTreePanel(final MLlibModel aDecisionTreeModel) {
        final DecisionTreeModel treeModel = (DecisionTreeModel) aDecisionTreeModel.getModel();
        final ColumnBasedValueMapping metaData = (ColumnBasedValueMapping)aDecisionTreeModel.getMetaData();
        final List<String> colNames = aDecisionTreeModel.getLearningColumnNames();
        final String classColName = aDecisionTreeModel.getTargetColumnName();

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
        final TreeNode rootNode = getRootNode(treeModel);
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
     * @param treeModel the {@link DecisionTreeModel}
     * @return the {@link TreeNode} that wraps the root node
     */
    protected TreeNode getRootNode(final DecisionTreeModel treeModel) {
        final TreeNode rootNode = new TreeNode2_4(treeModel.topNode());
        return rootNode;
    }
}
