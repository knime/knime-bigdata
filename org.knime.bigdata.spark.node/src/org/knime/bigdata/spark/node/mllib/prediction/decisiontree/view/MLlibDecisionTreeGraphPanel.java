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
 *   Created on May 25, 2016 by oole
 */
package org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view;

import java.awt.Color;
import java.awt.Component;
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

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.border.EmptyBorder;

import org.knime.base.node.mine.decisiontree2.view.graph.CollapseBranchAction;
import org.knime.base.node.mine.decisiontree2.view.graph.ExpandBranchAction;
import org.knime.core.data.property.ColorAttr;
import org.knime.core.node.NodeLogger;

import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeNodeModel;


/**
 * Creates the panel for the {@link MLlibDecisionTreeGraphView}
 *
 * @author Ole Ostergaard
 */
public class MLlibDecisionTreeGraphPanel extends JSplitPane {
    private static final long serialVersionUID = 4229032429584451491L;
    private static final NodeLogger LOGGER = NodeLogger.getLogger(MLlibDecisionTreeNodeModel.class);
    private MLlibDecisionTreeGraphView m_graph;
    private JPopupMenu m_popup;

    /**
     * @param decisionTreeModel The nodeModel for the {@link NodeLogger}
     * @param graph the graph to be displayed
     */
    public MLlibDecisionTreeGraphPanel(final MLlibDecisionTreeNodeModel decisionTreeModel, final MLlibDecisionTreeGraphView graph) {
        m_graph = graph;
        JPanel p = new JPanel(new GridLayout());
        p.setBackground(ColorAttr.BACKGROUND);
        p.add(m_graph.getView());
        p.setBorder(new EmptyBorder(5, 5, 5, 5));
        JScrollPane treeView = new JScrollPane(p);
        Dimension prefSize = treeView.getPreferredSize();
        treeView.setPreferredSize(new Dimension(Math.min(prefSize.width, 800), prefSize.height));

        setResizeWeight(1.0);
        setLeftComponent(treeView);
        setRightComponent(createRightPanel());

        m_popup = new JPopupMenu();
        m_popup.add(new ExpandBranchAction<>(m_graph));
        m_popup.add(new CollapseBranchAction<>(m_graph));

        m_graph.getView().addMouseListener(new MouseAdapter() {
            private void showPopup(final MouseEvent e) {
                TreeNode node = m_graph.nodeAtPoint(e.getPoint());
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

    /**
     * @return
     */
    private Component createRightPanel() {
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
        final Map<Object, Float> scaleFactors = new LinkedHashMap<>();
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
                validate();
                repaint();
            }
        });
        p.add(scaleFactorComboBox, c);
        return p;
    }
}
