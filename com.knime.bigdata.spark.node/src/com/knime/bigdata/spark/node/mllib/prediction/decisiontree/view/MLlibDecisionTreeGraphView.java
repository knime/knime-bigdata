/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   26.11.2010 (hofer): created
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree.view;

import java.awt.Dimension;
import java.awt.event.MouseEvent;
import java.util.Map;

import org.knime.base.node.mine.decisiontree2.view.graph.HierarchicalGraphView;
import org.knime.base.node.mine.decisiontree2.view.graph.NodeWidgetFactory;

import com.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;

/**
 * A hierarchical graph for decision trees.
 *
 * @author Ole Ostergaard
 */
public class MLlibDecisionTreeGraphView extends HierarchicalGraphView<TreeNode> {
    private MLlibDecisionTreeNodeWidgetFactory m_factory;
    private Map<Integer, String> m_featureMap;
    private ColumnBasedValueMapping m_metaData;

    /**
     * @param topNode the top/root node of the tree
     * @param features the map, mapping column numbers to feature names
     * @param metaData  the map, mapping the feature value ids to the feature value names
     */
    public MLlibDecisionTreeGraphView(final TreeNode topNode, final Map<Integer, String> features, final ColumnBasedValueMapping metaData) {
        super(topNode);
        m_featureMap = features;
        m_metaData = metaData;


        // fix to recreate nodewidgets including feature map
        getWidgets().clear();
        if (null != topNode) {
            MLlibDecisionTreeNodeWidget w = (MLlibDecisionTreeNodeWidget)getNodeWidgetFactoryWithFeatures(features, m_metaData).
            createGraphNode(topNode);
            setNodeWidth((int)w.getPreferredSize().getWidth() + 20);
            layoutGraph();
            // With this call parent components get informed about the
            // change in the preferred size.
            getView().revalidate();
            // make sure that the root node is in the visible area
            getView().scrollRectToVisible(getVisible().get(getRoot()));
        } else {
            setNodeWidth(100);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRootNode(final TreeNode root) {
        if (null != root) {
            MLlibDecisionTreeNodeWidget w = (MLlibDecisionTreeNodeWidget)getNodeWidgetFactory().
                                    createGraphNode(root);
            setNodeWidth((int)w.getPreferredSize().getWidth() + 20);
        } else {
            setNodeWidth(100);
        }
        super.setRootNode(root);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeWidgetFactory<TreeNode> getNodeWidgetFactory() {
        if (m_factory == null) {
            m_factory = new MLlibDecisionTreeNodeWidgetFactory(this);
        }
        return m_factory;
    }

    /**
     * @param features
     * @param metaData
     * @return nodeWidgetFactory
     */
    public NodeWidgetFactory<TreeNode> getNodeWidgetFactoryWithFeatures(final Map<Integer, String> features, final ColumnBasedValueMapping metaData) {
        m_featureMap = features;
        m_metaData = metaData;

        if (m_factory == null) {
            m_factory = new MLlibDecisionTreeNodeWidgetFactory(this);
        }
        return m_factory;

    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("null")
    @Override
    public void mousePressed(final MouseEvent e) {
        TreeNode nodePressed = nodeAtPoint(e.getPoint());
        Dimension preferredSize = null != nodePressed
                ? getWidgets().get(nodePressed).getPreferredSize()
                : null;
        super.mousePressed(e);
        // relayout when preferred size of the clicked node has changed
        Dimension preferredSizeAfter = null != nodePressed
            ? getWidgets().get(nodePressed).getPreferredSize()
            : null;
        if (null != nodePressed && !preferredSize.equals(preferredSizeAfter)) {
            layoutGraph();
            getView().revalidate();
            getView().repaint();
            // make sure that the clicked node is in the visible area
            getView().scrollRectToVisible(getVisible().get(nodePressed));
        }
    }

    /**
     * @return the map linking the column index to the features
     */
    public Map<Integer, String> getFeatureMap() {
        return m_featureMap;
    }

    /**
     * @return the value mapping for the graph, can be entered using the featureMap
     */
    public ColumnBasedValueMapping getMetaData() {
        return m_metaData;
    }
}
