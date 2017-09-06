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
 *   Created on 13.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.statistics.correlation.matrix;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 *
 * @author koetter
 */
public class MLlibCorrelationMatrixNodeFactory extends DefaultSparkNodeFactory<MLlibCorrelationMatrixNodeModel> {

    /**Constructor.*/
    public MLlibCorrelationMatrixNodeFactory() {
        super("statistics");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MLlibCorrelationMatrixNodeModel createNodeModel() {
        return new MLlibCorrelationMatrixNodeModel();
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
    public NodeView<MLlibCorrelationMatrixNodeModel> createNodeView(final int viewIndex,
        final MLlibCorrelationMatrixNodeModel nodeModel) {
        return new CorrelationComputeNodeView<>(nodeModel);
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
    protected NodeDialogPane createSparkNodeDialogPane() {
        return new MLlibCorrelationMatrixNodeDialog();
    }
}
