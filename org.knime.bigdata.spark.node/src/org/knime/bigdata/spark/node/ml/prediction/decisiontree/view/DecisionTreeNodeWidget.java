/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 *   22.07.2010 (hofer): created
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree.view;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Paint;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;

import org.knime.base.node.mine.decisiontree2.view.CollapsiblePanel;
import org.knime.base.node.mine.decisiontree2.view.graph.ComponentNodeWidget;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.core.data.property.ColorAttr;

/**
 * A view for a {@link TreeNode}.
 *
 * @author Ole Ostergaard
 */
public final class DecisionTreeNodeWidget extends ComponentNodeWidget<TreeNode> {
    private boolean m_displayTable;

    private boolean m_tableCollapsed;

    private boolean m_chartCollapsed;

    private CollapsiblePanel m_table;

    private CollapsiblePanel m_chart;

    private Map<Integer, String> m_featureMap;

    private ColumnBasedValueMapping m_metaData;

    private TreeNode m_rootNode;

    /**
     * @param graph the graph this widget is element of
     * @param decNode the model for this widget
     * @param tableCollapsed true when table should be collapsed initially
     * @param chartCollapsed true when chart should be collapsed initially
     */
    public DecisionTreeNodeWidget(final DecisionTreeGraphView graph, final TreeNode decNode,
        final boolean tableCollapsed, final boolean chartCollapsed) {
        super(graph, decNode);
        m_tableCollapsed = tableCollapsed;
        m_chartCollapsed = chartCollapsed;
        m_displayTable = true;
        m_featureMap = graph.getFeatureMap();
        m_metaData = graph.getMetaData();
        m_rootNode = graph.getRootNode();
    }

    /**
     * @param graph the graph this widget is element of
     * @param decNode the model for this widget
     * @param tableCollapsed true when table should be collapsed initially
     * @param chartCollapsed true when chart should be collapsed initially
     * @param displayTable true when the table should be displayed
     * @param displayChart true when the chart should be displayed
     */
    public DecisionTreeNodeWidget(final DecisionTreeGraphView graph, final TreeNode decNode,
        final boolean tableCollapsed, final boolean chartCollapsed, final boolean displayTable,
        final boolean displayChart) {
        super(graph, decNode);
        m_tableCollapsed = tableCollapsed;
        m_chartCollapsed = chartCollapsed;
        m_displayTable = displayTable;
        m_featureMap = graph.getFeatureMap();
        m_metaData = graph.getMetaData();
        m_rootNode = graph.getRootNode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JComponent createComponent() {
        float scale = getScaleFactor();
        JPanel p = new JPanel(new GridBagLayout());
        p.setOpaque(false);
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.insets = new Insets(0, 0, 0, 0);
        c.gridx = 0;
        c.gridy = 0;
        c.weightx = 1;
        c.weighty = 1;
        JPanel nodePanel = createNodeLabelPanel(scale);
        nodePanel.setOpaque(false);
        p.add(nodePanel, c);
        // add table
        TreeNode node = getUserObject();

        if (m_displayTable && !node.isLeaf()) {
            c.gridy++;
            JPanel tablePanel = createTablePanel(scale);
            tablePanel.setOpaque(false);
            m_table = new CollapsiblePanel("Statistics:", tablePanel, scale);
            m_table.setOpaque(false);
            m_table.setCollapsed(m_tableCollapsed);
            p.add(m_table, c);
        }

        return p;
    }

    /**
     * @return the tableCollapsed
     */
    boolean getTableCollapsed() {
        return null != m_table ? m_table.isCollapsed() : m_tableCollapsed;
    }

    /**
     * @param collapsed the tableCollapsed to set
     */
    void setTableCollapsed(final boolean collapsed) {
        m_tableCollapsed = collapsed;
        if (m_table != null) {
            m_table.setCollapsed(collapsed);
        }
    }

    /**
     * @return the chartCollapsed
     */
    boolean getChartCollapsed() {
        return null != m_chart ? m_chart.isCollapsed() : m_chartCollapsed;
    }

    /**
     * @param collapsed the chartCollapsed to set
     */
    void setChartCollapsed(final boolean collapsed) {
        m_chartCollapsed = collapsed;
        if (m_chart != null) {
            m_chart.setCollapsed(collapsed);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setScaleFactor(final float scale) {
        // remember if table is collapsed
        if (null != m_table) {
            m_tableCollapsed = m_table.isCollapsed();
        }
        // remember if chart is collapsed
        if (null != m_chart) {
            m_chartCollapsed = m_chart.isCollapsed();
        }
        super.setScaleFactor(scale);
    }

    /**
     * The panel at the top displaying the node label.
     *
     * @param scale
     * @return A label, e.g. "Iris-versicolor (45/46)"
     */
    private JPanel createNodeLabelPanel(final float scale) {
        int gap = Math.round(5 * scale);
        JPanel p = new JPanel(new FlowLayout(FlowLayout.CENTER, gap, gap));
        TreeNode node = getUserObject();
        StringBuilder label = new StringBuilder();
        // if there is a mapping present we try to map the values
        if (m_metaData != null) {
            double pred = node.getPrediction();
            // if we find a mapped value we append it, else we just append the prediction
            if (m_metaData.map(m_featureMap.size() - 1, node.getPrediction()) != null) {
                label.append((String)m_metaData.map(m_featureMap.size() - 1, node.getPrediction()));
            } else {
                label.append(convertCount(pred));
            }
        } else {
            label.append(convertCount(node.getPrediction()));
        }
        // this is a fix for the gradient boosted tree in spark 1.2
        if (node.getProbability() != -1) {
            label.append(" (");
            label.append(convertCount(node.getProbability()));
            label.append(")");
        }
        JLabel l = scaledLabel(label.toString(), scale);
        if (node.isLeaf()) {
            l.setFont(l.getFont().deriveFont(l.getFont().getStyle() | Font.BOLD));
        }
        p.add(l);
        return p;
    }

    // The table
    private JPanel createTablePanel(final float scale) {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        int gridwidth = 2;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        int bw = Math.round(1 * scale);
        c.insets = new Insets(bw, bw, bw, bw);
        c.gridx = 0;
        c.gridy = 0;
        c.weightx = 1;
        c.weighty = 1;
        c.gridwidth = 1;
        p.add(scaledLabel("Information", scale), c);
        c.gridx++;
        p.add(scaledLabel("Value ", scale, SwingConstants.RIGHT), c);
        c.gridy++;
        c.gridx = 0;
        c.gridwidth = GridBagConstraints.REMAINDER;
        p.add(new MyJSeparator(), c);
        c.gridwidth = 1;

        // Information is entered here
        TreeNode node = getUserObject();
        if (node.hasStats()) {
            c.gridy++;
            c.gridx = 0;
            JLabel gainLabel = scaledLabel("Gain", scale);
            p.add(gainLabel, c);
            c.gridx++;
            JLabel gainValue = scaledLabel(convertCount(node.getGain()), scale);
            p.add(gainValue, c);
        }
        c.gridy++;
        c.gridx = 0;
        JLabel impurityLabel = scaledLabel("Impurity", scale);
        p.add(impurityLabel, c);
        c.gridx++;
        JLabel impurityValue = scaledLabel(convertCount(node.getImpurity()), scale);
        p.add(impurityValue, c);
        c.gridy++;
        c.gridx = 0;
        if (node.hasStats()) {
            JLabel leftImpLabel = scaledLabel("LeftImpurity", scale);
            p.add(leftImpLabel, c);
            c.gridx++;
            JLabel leftImpValue = scaledLabel(convertCount(node.getLeftImpurity()), scale);
            p.add(leftImpValue, c);
            c.gridy++;
            c.gridx = 0;
            JLabel rightImpLabel = scaledLabel("RightImpurity", scale);
            p.add(rightImpLabel, c);
            c.gridx++;
            JLabel rightImpValue = scaledLabel(convertCount(node.getRightImpurity()), scale);
            p.add(rightImpValue, c);
            c.gridy++;
            c.gridx = 0;
        }
        JLabel descendantsLabel = scaledLabel("Descendants", scale);
        p.add(descendantsLabel, c);
        c.gridx++;
        JLabel descendantsValue = scaledLabel(node.numDescendants().toString(), scale);
        p.add(descendantsValue, c);
        c.gridy++;
        c.gridx = 0;
        c.gridwidth = gridwidth;
        p.add(new MyJSeparator(), c);
        return p;
    }

    private static JLabel scaledLabel(final String label, final float scale) {
        JLabel l = new JLabel(label);
        l.setFont(l.getFont().deriveFont(l.getFont().getSize() * scale));
        return l;
    }

    private static JLabel scaledLabel(final String label, final float scale, final int horizontalAlignment) {
        JLabel l = new JLabel(label, horizontalAlignment);
        l.setFont(l.getFont().deriveFont(l.getFont().getSize() * scale));
        return l;
    }

    private static final DecimalFormat DECIMAL_FORMAT_ONE = initFormat("0.000");

    private static final DecimalFormat DECIMAL_FORMAT = initFormat("0");

    private static DecimalFormat initFormat(final String pattern) {
        DecimalFormat df = new DecimalFormat(pattern);
        df.setGroupingUsed(true);
        df.setGroupingSize(3);
        return df;
    }

    private static String convertCount(final double value) {
        // show integer as integer (without decimal places)
        if (Double.compare(Math.ceil(value), value) == 0) {
            return DECIMAL_FORMAT.format(value);
        }
        return DECIMAL_FORMAT_ONE.format(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConnectorLabelAbove() {
        TreeNode node = getUserObject();
        if (!node.equals(m_rootNode)) {
            if (node.isCategorical()) {
                return getCategoricalSplitLabel(node);
            } else {
                return getContinuousSplitLabel(node);
            }
        } else {
            return null;
        }
    }

    /**
     * @param node
     * @return
     */
    private String getCategoricalSplitLabel(final TreeNode node) {

        List<String> categories = new LinkedList<>();
        for (Object categoryValue : node.getCategories()) {
            if (m_metaData != null) {
                final Object nominalValue = m_metaData.map(node.getParentSplitFeature(), categoryValue);
                if (nominalValue != null) {
                    categories.add(nominalValue.toString());
                }
            } else {
                categories.add(categoryValue.toString());
            }
        }

        final String category = String.format("{%s}", String.join(", ", categories));

        if (node.isLeftChild()) {
            return "=" + category.toString();
        } else {
            return "\u2260" + category.toString();
        }
    }

    private static String getContinuousSplitLabel(final TreeNode node) {
        final String continuous = convertCount(node.getThreshold());
        if (node.isLeftChild()) {
            return "\u2264 " + continuous;
        } else {
            return "> " + continuous;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConnectorLabelBelow() {
        TreeNode node = getUserObject();
        if (!node.isLeaf()) {
            String feature = m_featureMap.get(node.getSplitFeature());
            return feature;
        } else {
            return null;
        }
    }

    private static class MyJSeparator extends JComponent {
        private static final long serialVersionUID = -5611048590057773103L;

        /**
         * {@inheritDoc}
         */
        @Override
        public Dimension getPreferredSize() {
            return new Dimension(2, 2);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Dimension getMaximumSize() {
            return getPreferredSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Dimension getMinimumSize() {
            return getPreferredSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void paintComponent(final Graphics g) {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D)g;
            Paint origPaint = g2.getPaint();
            g2.setPaint(ColorAttr.BORDER);
            Dimension s = getSize();
            g2.drawLine(0, s.height / 2, s.width, s.height / 2);
            g2.setPaint(origPaint);
        }
    }
}
