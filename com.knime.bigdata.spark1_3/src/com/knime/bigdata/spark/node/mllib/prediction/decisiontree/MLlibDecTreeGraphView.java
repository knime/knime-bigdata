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
 *   Created on 30.09.2015 by dwk
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.swing.Action;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.border.EmptyBorder;

import org.knime.base.node.mine.decisiontree2.model.DecisionTreeNode;
import org.knime.base.node.mine.decisiontree2.view.DecTreeGraphView;
import org.knime.base.node.mine.decisiontree2.view.graph.CollapseBranchAction;
import org.knime.base.node.mine.decisiontree2.view.graph.ExpandBranchAction;
import org.knime.core.data.property.ColorAttr;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

/**
 * The graph view displayed the view of the Decision Tree to Image node.
 *
 * @author Heiko Hofer, copied and modified by dwk
 * TODO - this is a much simplified version of org.knime.base.node.mine.decisiontree2.learner2.DecTreeLearnerGraphView2,
 *    we might want to sub-class instead
 */
class MLlibDecTreeGraphView extends NodeView<MLlibDecisionTreeNodeModel> {

    /** The node logger for this class. */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(MLlibDecTreeGraphView.class);

    private DecTreeGraphView m_graph;

    private JPopupMenu m_popup;

    /**
     * Default constructor, taking the model as argument.
     *
     * @param model the underlying NodeModel
     */
    MLlibDecTreeGraphView(final MLlibDecisionTreeNodeModel model, final DecTreeGraphView aGraphView) {
        super(model);
        m_graph = aGraphView;
        JPanel p = new JPanel(new GridLayout());
        p.setBackground(ColorAttr.BACKGROUND);
        p.add(m_graph.getView());
        p.setBorder(new EmptyBorder(5, 5, 5, 5));
        JScrollPane treeView = new JScrollPane(p);
        Dimension prefSize = treeView.getPreferredSize();
        treeView.setPreferredSize(new Dimension(Math.min(prefSize.width, 800), prefSize.height));

        JSplitPane splitPane = new JSplitPane();
        splitPane.setResizeWeight(1.0);
        splitPane.setLeftComponent(treeView);
        splitPane.setRightComponent(createRightPanel());

        setComponent(splitPane);

        // add menu entries for tree operations
        this.getJMenuBar().add(createTreeMenu());

        m_popup = new JPopupMenu();
        m_popup.add(new ExpandBranchAction<DecisionTreeNode>(m_graph));
        m_popup.add(new CollapseBranchAction<DecisionTreeNode>(m_graph));

        m_graph.getView().addMouseListener(new MouseAdapter() {
            private void showPopup(final MouseEvent e) {
                DecisionTreeNode node = m_graph.nodeAtPoint(e.getPoint());
                if (null != node) {
                    m_popup.show(m_graph.getView(), e.getX(), e.getY());
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void mousePressed(final MouseEvent e) {
                if (e.isPopupTrigger()) {
                    showPopup(e);
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void mouseClicked(final MouseEvent e) {
                if (e.isPopupTrigger()) {
                    showPopup(e);
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void mouseReleased(final MouseEvent e) {
                if (e.isPopupTrigger()) {
                    showPopup(e);
                }
            }
        });
    }

    JComponent getView() {
        return (JComponent)getComponent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Container getExportComponent() {
        return m_graph.getView();
    }

    /* Create the Panel with the outline view and the controls */
    private JPanel createRightPanel() {
        JPanel p = new JPanel(new GridBagLayout());
        p.setBackground(Color.white);
        GridBagConstraints c = new GridBagConstraints();

        c.fill = GridBagConstraints.BOTH;
        c.anchor = GridBagConstraints.WEST;
        c.insets = new Insets(6, 6, 4, 6);
        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 1;
        c.weightx = 1.0;
        c.weighty = 1.0;

        p.add(m_graph.createOutlineView(), c);

        c.weighty = 0;
        c.gridy++;
        p.add(new JLabel("Zoom:"), c);

        c.gridy++;
        final Map<Object, Float> scaleFactors = new LinkedHashMap<Object, Float>();
        scaleFactors.put("140.0%", 140f);
        scaleFactors.put("120.0%", 120f);
        scaleFactors.put("100.0%", 100f);
        scaleFactors.put("80.0%", 80f);
        scaleFactors.put("60.0%", 60f);

        final JComboBox<Object> scaleFactorComboBox = new JComboBox<>(scaleFactors.keySet().toArray());
        scaleFactorComboBox.setEditable(true);
        scaleFactorComboBox.setSelectedItem("100.0%");
        scaleFactorComboBox.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                Object selected = scaleFactorComboBox.getSelectedItem();
                Float scaleFactor = scaleFactors.get(selected);
                if (null == scaleFactor) {
                    String str = ((String)selected).trim();
                    if (str.endsWith("%")) {
                        scaleFactor = Float.parseFloat(str.substring(0, str.length() - 1));
                    } else {
                        scaleFactor = Float.parseFloat(str);
                    }
                }
                if (scaleFactor < 10) {
                    LOGGER.error("A zoom which is lower than 10% " + "is not supported");
                    scaleFactor = 10f;
                }
                if (scaleFactor > 500) {
                    LOGGER.error("A zoom which is greater than 500% " + "is not supported");
                    scaleFactor = 500f;
                }
                String sf = Float.toString(scaleFactor) + "%";
                scaleFactorComboBox.setSelectedItem(sf);
                scaleFactor = scaleFactor / 100f;
                m_graph.setScaleFactor(scaleFactor);
                getComponent().validate();
                getComponent().repaint();
            }
        });
        p.add(scaleFactorComboBox, c);
        return p;
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

    /**
     * {@inheritDoc}
     */
    @Override
    protected void modelChanged() {
        // do nothing
    }


    /**
     * Create menu to control tree.
     *
     * @return A new JMenu with tree operation buttons
     */
    private JMenu createTreeMenu() {
        final JMenu result = new JMenu("Tree");
        result.setMnemonic('T');

        Action expand = new ExpandBranchAction<DecisionTreeNode>(m_graph);
        expand.putValue(Action.NAME, "Expand Selected Branch");
        Action collapse = new CollapseBranchAction<DecisionTreeNode>(m_graph);
        collapse.putValue(Action.NAME, "Collapse Selected Branch");
        result.add(expand);
        result.add(collapse);

        return result;
    }
}
