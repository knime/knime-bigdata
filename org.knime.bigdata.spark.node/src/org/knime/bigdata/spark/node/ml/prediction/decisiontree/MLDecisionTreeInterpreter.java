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
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingWorker;

import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaDataUtils;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.MLlibDecisionTreeGraphPanel;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.MLlibDecisionTreeGraphView;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.TreeNode;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

/**
 * @author Ole Ostergaard
 */
public class MLDecisionTreeInterpreter implements ModelInterpreter<MLModel> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(MLDecisionTreeInterpreter.class);

    private static final long serialVersionUID = 1L;

    private final MLModelType m_modelType;

    /**
     * @param modelType The type of the model to interpret.
     */
    public MLDecisionTreeInterpreter(final MLModelType modelType) {
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
    public String getSummary(final MLModel pipelineModel) {
        final MLDecisionTreeMetaData metaData = pipelineModel.getModelMetaData(MLDecisionTreeMetaData.class).get();
        return String.format("Tree depth: %d / Number of nodes: %d", metaData.getTreeDepth(),
            metaData.getNumberOfTreeNodes());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final MLModel mlModel) {
        return new JComponent[]{getTreePanel(mlModel)};
    }

    private static JComponent getTreePanel(final MLModel decisionTreeModel) {

        final List<String> colNames = decisionTreeModel.getLearningColumnNames();
        final String classColName = decisionTreeModel.getTargetColumnName().get();

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

                final MLDecisionTree tree;
                try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(decisionTreeModel.getModelInterpreterFile().get())))) {

                    tree = MLDecisionTree.read(in);
                }

                return MLDecisionTreeInterpreter.getTreeView(tree.getRootNode(), colNames, classColName,
                    decisionTreeModel);
            }

            /** {@inheritDoc} */
            @Override
            protected void done() {
                JComponent dt = null;
                try {
                    dt = super.get();
                } catch (ExecutionException | InterruptedException ee) {
                    LOGGER.warn("Error reading model, reason: " + ee.getMessage(), ee);
                    final Throwable cause = ee.getCause();
                    final String msg;
                    if (cause != null) {
                        msg = cause.getMessage();
                    } else {
                        msg = ee.getMessage();
                    }
                    treePanel.removeAll();
                    treePanel.add(new JLabel("Error reading model: " + msg), BorderLayout.NORTH);
                    treePanel.repaint();
                    treePanel.revalidate();
                    return;
                }
                if (dt == null) {
                    treePanel.removeAll();
                    treePanel.add(new JLabel("Error reading model. For details see log file."),
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
     * converts the given tree model into PMML and packs it into a JComponent
     *
     * @param rootNode
     * @param aColNames
     * @param aClassColName
     * @param mlModel
     * @return displayable component
     */
    public static JComponent getTreeView(final TreeNode rootNode, final List<String> aColNames,
        final String aClassColName, final MLModel mlModel) {

        final Map<Integer, String> features = new HashMap<>();
        int ctr =0;
        for (String col : aColNames) {
            features.put(ctr++, col);
        }
        features.put(ctr, aClassColName);

        final ColumnBasedValueMapping legacyMapping = MLMetaDataUtils.toLegacyColumnBasedValueMapping(mlModel);

        final MLlibDecisionTreeGraphView graph = new MLlibDecisionTreeGraphView(rootNode, features, legacyMapping);
        final JComponent view = new MLlibDecisionTreeGraphPanel(graph);
        view.setName("Decision Tree View");
        return view;
    }

}
