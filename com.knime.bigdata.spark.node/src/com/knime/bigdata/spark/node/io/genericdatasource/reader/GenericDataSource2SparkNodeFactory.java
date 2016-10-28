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
 *   Created on Aug 10, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.reader;

import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 * @author Sascha Wolke, KNIME.com
 * @param <M> Model of this node factory
 * @param <S> Settings of this node factory
 */
public abstract class GenericDataSource2SparkNodeFactory <M extends GenericDataSource2SparkNodeModel<S>, S extends GenericDataSource2SparkSettings> extends DefaultSparkNodeFactory<M> {

    /** Default Constructor. */
    public GenericDataSource2SparkNodeFactory() {
        super("io/read");
    }

    /** @return Initial settings object */
    public abstract S getSettings();

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<M> createNodeView(final int viewIndex, final M nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }
}
