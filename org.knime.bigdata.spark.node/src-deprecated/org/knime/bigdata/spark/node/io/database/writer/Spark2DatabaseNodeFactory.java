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
package org.knime.bigdata.spark.node.io.database.writer;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 * @author Sascha Wolke, KNIME.com
 */
@Deprecated
public class Spark2DatabaseNodeFactory extends DefaultSparkNodeFactory<Spark2DatabaseNodeModel> {

    /** Default constructor. */
    public Spark2DatabaseNodeFactory() {
        super("io/db");
    }

    @Override
    public Spark2DatabaseNodeModel createNodeModel() {
        return new Spark2DatabaseNodeModel();
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<Spark2DatabaseNodeModel> createNodeView(final int viewIndex, final Spark2DatabaseNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Spark2DatabaseNodeDialog();
    }
}
