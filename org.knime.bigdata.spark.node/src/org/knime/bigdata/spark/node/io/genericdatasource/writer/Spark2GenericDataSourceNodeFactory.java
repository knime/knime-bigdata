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
 *   Created on Sep 05, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.writer;

import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 * Default spark 2 generic data source node factory.
 *
 * @author Sascha Wolke, KNIME.com
 * @param <M> Model of this node factory
 * @param <S> Settings of this node factory
 */
public abstract class Spark2GenericDataSourceNodeFactory <M extends Spark2GenericDataSourceNodeModel<S>, S extends Spark2GenericDataSourceSettings> extends DefaultSparkNodeFactory<M> {

    /** Default constructor. */
    public Spark2GenericDataSourceNodeFactory() {
        super("io/write");
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
