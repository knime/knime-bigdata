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
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package com.knime.bigdata.spark.node.scorer.numeric;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkNumericScorerNodeFactory extends DefaultSparkNodeFactory<SparkNumericScorerNodeModel> {

    /**
     * Constructor.
     */
    public SparkNumericScorerNodeFactory() {
        super("mining/scoring");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkNumericScorerNodeModel createNodeModel() {
        return new SparkNumericScorerNodeModel();
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
    public NodeView<SparkNumericScorerNodeModel> createNodeView(final int viewIndex, final SparkNumericScorerNodeModel nodeModel) {
        if (viewIndex == 0) {
            return new SparkNumericScorerNodeView(nodeModel);
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
        return new SparkNumericScorerNodeDialog();
    }
}
