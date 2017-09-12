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
 *   Created on Sep 05, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.database.reader;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Database2SparkNodeFactory extends DefaultSparkNodeFactory<Database2SparkNodeModel> {

    /** Default constructor. */
    public Database2SparkNodeFactory() {
        super("io/db");
    }

    @Override
    public Database2SparkNodeModel createNodeModel() {
        return new Database2SparkNodeModel(true);
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<Database2SparkNodeModel> createNodeView(final int viewIndex, final Database2SparkNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createSparkNodeDialogPane() {
        return new Database2SparkNodeDialog();
    }
}
