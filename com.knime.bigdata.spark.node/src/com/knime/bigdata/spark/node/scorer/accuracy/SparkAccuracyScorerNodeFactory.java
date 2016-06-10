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
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package com.knime.bigdata.spark.node.scorer.accuracy;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkAccuracyScorerNodeFactory extends DefaultSparkNodeFactory<SparkAccuracyScorerNodeModel> {



    /**
     *  Constructor.
     */
    public SparkAccuracyScorerNodeFactory() {
        super("mining/scoring");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkAccuracyScorerNodeModel createNodeModel() {
        return new SparkAccuracyScorerNodeModel();
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
    public NodeView<SparkAccuracyScorerNodeModel> createNodeView(final int viewIndex, final SparkAccuracyScorerNodeModel nodeModel) {
        if (viewIndex == 0) {
            return new SparkAccuracyScorerNodeView(nodeModel);
        } else {
            throw new IllegalArgumentException("No such view");
        }
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
        return new SparkAccuracyScorerNodeDialog();
    }
}
