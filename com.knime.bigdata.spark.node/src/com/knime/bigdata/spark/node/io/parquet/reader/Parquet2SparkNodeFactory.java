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
package com.knime.bigdata.spark.node.io.parquet.reader;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Parquet2SparkNodeFactory extends DefaultSparkNodeFactory<Parquet2SparkNodeModel> {

    /**
     * Constructor.
     */
    public Parquet2SparkNodeFactory() {
        super("io/write");
    }

    @Override
    public Parquet2SparkNodeModel createNodeModel() {
        return new Parquet2SparkNodeModel();
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<Parquet2SparkNodeModel> createNodeView(final int viewIndex, final Parquet2SparkNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Parquet2SparkNodeDialog();
    }
}
