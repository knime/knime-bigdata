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
 *   Created on 03.08.2015 by dwk
 */
package com.knime.bigdata.spark.node.preproc.pmml.normalize;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 *
 * @author dwk
 */
public class SparkNormalizerPMMLNodeFactory extends NodeFactory<SparkNormalizerPMMLNodeModel> {
    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new SparkNormalizerPMMLNodeDialog();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkNormalizerPMMLNodeModel createNodeModel() {
        return new SparkNormalizerPMMLNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<SparkNormalizerPMMLNodeModel> createNodeView(final int viewIndex, final SparkNormalizerPMMLNodeModel nodeModel) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

}
