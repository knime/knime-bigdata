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
 *   Created on Sep 30, 2015 by bjoern
 */
package org.knime.bigdata.spark.node.scorer.entropy;

import org.knime.base.node.mine.scorer.entrop.EntropyNodeDialogPane;
import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeView;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkEntropyScorerNodeFactory extends DefaultSparkNodeFactory<SparkEntropyScorerNodeModel> {

    /**
     * Constructor.
     */
    public SparkEntropyScorerNodeFactory() {
        super("mining/scoring");
    }

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
    public NodeView<SparkEntropyScorerNodeModel> createNodeView(final int viewIndex,
        final SparkEntropyScorerNodeModel nodeModel) {
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
     * This extends the {@link EntropyNodeDialogPane} for the {@link SparkEntropyScorerNodeModel}
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new EntropyNodeDialogPane(true) {
            /**
             * {@inheritDoc}
             */
            @Override
            protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
                throws NotConfigurableException {
                super.loadSettingsFrom(settings, new DataTableSpec[]{((SparkDataPortObjectSpec)specs[0]).getTableSpec(),
                    ((SparkDataPortObjectSpec)specs[0]).getTableSpec()});
            }
        };
    }

}
