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
package com.knime.bigdata.spark.node.io.parquet.writer;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2ParquetNodeFactory extends DefaultSparkNodeFactory<Spark2ParquetNodeModel> {

    /** Constructor */
    public Spark2ParquetNodeFactory() {
        super("io/read");
    }

    @Override
    public Spark2ParquetNodeModel createNodeModel() {
        return new Spark2ParquetNodeModel();
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<Spark2ParquetNodeModel> createNodeView(final int viewIndex, final Spark2ParquetNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Spark2ParquetNodeDialog();
    }
}
