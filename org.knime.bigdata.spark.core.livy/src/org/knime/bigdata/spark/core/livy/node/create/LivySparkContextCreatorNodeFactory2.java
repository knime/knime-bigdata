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
 *   Created on 03.07.2015 by koetter
 */
package org.knime.bigdata.spark.core.livy.node.create;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LivySparkContextCreatorNodeFactory2 extends DefaultSparkNodeFactory<LivySparkContextCreatorNodeModel2> {

    /**
     * Constructor.
     */
    public LivySparkContextCreatorNodeFactory2() {
        super("");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LivySparkContextCreatorNodeModel2 createNodeModel() {
        return new LivySparkContextCreatorNodeModel2();
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
        return new LivySparkContextCreatorNodeDialog(false);
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
    public NodeView<LivySparkContextCreatorNodeModel2> createNodeView(final int viewIndex,
        final LivySparkContextCreatorNodeModel2 nodeModel) {
        if (viewIndex == 0) {
            return new LivySparkContextLogView<>(nodeModel);
        } else {
            throw new IllegalArgumentException("No such view");
        }
    }

}
