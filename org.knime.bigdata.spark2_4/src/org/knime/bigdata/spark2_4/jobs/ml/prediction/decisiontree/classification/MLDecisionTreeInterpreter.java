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
package org.knime.bigdata.spark2_4.jobs.ml.prediction.decisiontree.classification;

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
import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeNodeModel;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.MLlibDecisionTreeGraphPanel;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.MLlibDecisionTreeGraphView;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.TreeNode;
import org.knime.bigdata.spark2_4.jobs.ml.prediction.decisiontree.MLTreeNode2_4;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

/**
 * @author Ole Ostergaard
 */
public class MLDecisionTreeInterpreter implements ModelInterpreter<MLModel> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(MLDecisionTreeInterpreter.class);

    private static final long serialVersionUID = 1L;

    private static final HashMap<String, MLDecisionTreeInterpreter> interpreterInstances = new HashMap<>();

    private final String m_modelName;

    private MLDecisionTreeInterpreter(final String modelName) {
        m_modelName = modelName;
    }

    /**
     * Returns the only instance of this class.
     *
     * @param modelName Specific model name we want to get the model interpreter for.
     * @return the singleton instance for the given model name
     */
    public synchronized static MLDecisionTreeInterpreter getInstance(final String modelName) {
        MLDecisionTreeInterpreter instance = interpreterInstances.get(modelName);
        if (instance == null) {
            instance = new MLDecisionTreeInterpreter(modelName);
            interpreterInstances.put(modelName, instance);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return m_modelName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final MLModel pipelineModel) {
        //FIXME
        return "FIXME";
//        final DecisionTreeModel treeModel = (DecisionTreeModel)model.getModel();
//        return String.format("Tree depth: %d / Number of nodes: %d", treeModel.depth(), treeModel.numNodes());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final MLModel pipelineModel) {
        return new JComponent[]{getTreePanel(pipelineModel)};
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

    private JComponent getTreePanel(final MLModel decisionTreePipelineModel) {

        final List<String> colNames = decisionTreePipelineModel.getLearningColumnNames();
        final String classColName = decisionTreePipelineModel.getTargetColumnName().get();

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
        treePanel.add(new JLabel("Loading model ..."), BorderLayout.NORTH);
        treePanel.repaint();
        treePanel.revalidate();

        final SwingWorker<JComponent, Void> worker = new SwingWorker<JComponent, Void>() {
            /** {@inheritDoc} */
            @Override
            protected JComponent doInBackground() throws Exception {
                throw new UnsupportedOperationException("Viewing Decision Tree models is not yet supported");
//                return new JLabel(buf.toString());
//                final TreeNode rootNode = getRootNode(treeModel);
//                return MLDecisionTreeInterpreter.getTreeView(rootNode, colNames, classColName, metaData);
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
        final TreeNode rootNode = new MLTreeNode2_4(treeModel.topNode());
        return rootNode;
    }
}
