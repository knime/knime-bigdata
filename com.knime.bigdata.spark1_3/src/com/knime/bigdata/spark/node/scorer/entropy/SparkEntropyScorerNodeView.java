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
 *   Created on Sep 30, 2015 by bjoern
 */
package com.knime.bigdata.spark.node.scorer.entropy;

import javax.swing.JEditorPane;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;

import org.knime.base.node.util.DoubleFormat;
import org.knime.core.node.NodeView;
import org.knime.core.node.tableview.TableView;

/**
 * Node view of the Spark Entropy Scorer node, which displays some quality statistics about a clustering.
 *
 * TODO: this is in large parts a code duplicate of {@link org.knime.base.node.mine.scorer.entrop.EntropyNodeView}
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkEntropyScorerNodeView extends NodeView<SparkEntropyScorerNodeModel> {

	/** The top level split pane hold some text and a table view */
    private final JSplitPane m_pane;

    /** The table view containing cluster statistics. */
    private final TableView m_tableView;

    /** The text pane that holds the information. */
    private final JEditorPane m_editorPane;

    /** Scrollpane containing m_editorPane. */
    private final JScrollPane m_editorPaneScroller;

    /**
     *
     * @param model the node model from which of display results.
     */
    public SparkEntropyScorerNodeView(final SparkEntropyScorerNodeModel model) {
        super(model);
        m_pane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        m_editorPane = new JEditorPane("text/html", "");
        m_editorPane.setEditable(false);
        m_tableView = new TableView();
        m_tableView.setShowColorInfo(false);
        m_editorPaneScroller = new JScrollPane(m_editorPane);
        m_pane.setTopComponent(m_editorPaneScroller);
        m_pane.setBottomComponent(m_tableView);
        setComponent(m_pane);
    }

    /**
     * Updates the view with the current model data.
     */
    public void update() {
        final SparkEntropyScorerViewData viewData = getNodeModel().getViewData();

        m_editorPane.setText("");
        if (viewData == null) {
            m_tableView.setDataTable(null);
            return;
        }
        m_tableView.setDataTable(viewData.getScoreTable());
        StringBuffer buffer = new StringBuffer();
        buffer.append("<html>");
        buffer.append("<body>\n");
        buffer.append("<h1>Clustering statistics</h1>");
        buffer.append("<hr>\n");
        buffer.append("<table>\n");
        buffer.append("<caption style=\"text-align: left\">");
        buffer.append("Data Statistics</caption>");
        buffer.append("<tr>");
        buffer.append("<th>Statistics</th>");
        buffer.append("<th>Value</th>");
        buffer.append("</tr>");
        String[] stats = new String[]{"Number of clusters found: ",
                "Number of objects in clusters: ",
                "Number of reference clusters: ", "Total number of patterns: "};
        int[] vals = new int[]{viewData.getNrClusters(),
                viewData.getOverallSize(),
                viewData.getNrReferenceClusters(),
                viewData.getOverallSize()};
        for (int i = 0; i < stats.length; i++) {
            buffer.append("<tr>\n");
            buffer.append("<td>\n");
            buffer.append(stats[i]);
            buffer.append("\n</td>\n");
            buffer.append("<td>\n");
            buffer.append(vals[i]);
            buffer.append("\n</td>\n");
            buffer.append("</tr>\n");
        }
        buffer.append("</table>\n");
        buffer.append("<table>\n");
        buffer.append("<caption style=\"text-align: left\">");
        buffer.append("Data Statistics</caption>");
        buffer.append("<tr>");
        buffer.append("<th>Score</th>");
        buffer.append("<th>Value</th>");
        buffer.append("</tr>");
        buffer.append("<tr>\n");
        buffer.append("<td>\n");
        buffer.append("Entropy: ");
        buffer.append("\n</td>\n");
        buffer.append("<td>\n");
        buffer.append(DoubleFormat.formatDouble(viewData.getOverallEntropy()));
        buffer.append("\n</td>\n");
        buffer.append("</tr>\n");
        buffer.append("<tr>\n");
        buffer.append("<td>\n");
        buffer.append("Quality: ");
        buffer.append("\n</td>\n");
        buffer.append("<td>\n");
        buffer.append(DoubleFormat.formatDouble(viewData.getOverallQuality()));
        buffer.append("\n</td>\n");
        buffer.append("</tr>\n");
        buffer.append("</table>\n");
        buffer.append("</body>\n");
        buffer.append("</html>\n");
        m_editorPane.setText(buffer.toString());
        // Do not call m_editorPane.getPreferredSize, bug in java:
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4839118
        int preferredEditorSize = m_editorPaneScroller.getPreferredSize().height + 10;
        if (m_pane.getSize().height > preferredEditorSize) {
            m_pane.setDividerLocation(preferredEditorSize);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void modelChanged() {
        update();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onClose() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onOpen() {
    }
}
