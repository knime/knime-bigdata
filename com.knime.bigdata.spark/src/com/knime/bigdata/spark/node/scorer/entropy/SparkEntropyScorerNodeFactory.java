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

import org.knime.base.node.mine.scorer.entrop.EntropyNodeDialogPane;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeView;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;


/**
 *
 * @author bjoern
 */
public class SparkEntropyScorerNodeFactory extends NodeFactory<SparkEntropyScorerNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkEntropyScorerNodeModel createNodeModel() {
        return new SparkEntropyScorerNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNrNodeViews() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<SparkEntropyScorerNodeModel> createNodeView(final int viewIndex, final SparkEntropyScorerNodeModel nodeModel) {
        return new SparkEntropyScorerNodeView(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new EntropyNodeDialogPane() {
            /**
             * {@inheritDoc}
             */
            @Override
            protected void loadSettingsFrom(final NodeSettingsRO settings,
                    final PortObjectSpec[] specs) throws NotConfigurableException {
                super.loadSettingsFrom(settings, new DataTableSpec[] { ((SparkDataPortObjectSpec) specs[0]).getTableSpec(), ((SparkDataPortObjectSpec) specs[1]).getTableSpec()} );
            }

        };
//
//        pane.addDialogComponent(new DialogComponentColumnNameSelection(new SettingsModelString(SparkEntropyScorerNodeModel.CFG_REFERENCE_COLUMN, null),
//            "Reference Column",
//            0,
//            new Class[] { StringValue.class }));
//
//        pane.addDialogComponent(new DialogComponentColumnNameSelection(new SettingsModelString(SparkEntropyScorerNodeModel.CFG_CLUSTERING_COLUMN, null),
//            "Clustering Column",
//            0,
//            new Class[] { StringValue.class }));
//
//        return pane;
    }

}
