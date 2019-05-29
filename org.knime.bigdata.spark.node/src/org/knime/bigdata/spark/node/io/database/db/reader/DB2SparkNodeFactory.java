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
 *   Created on Mai 26, 2019 by Mareike
 */
package org.knime.bigdata.spark.node.io.database.db.reader;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class DB2SparkNodeFactory extends DefaultSparkNodeFactory<DB2SparkNodeModel> {

    /** Default constructor. */
    public DB2SparkNodeFactory() {
        super("io/db");
    }

    @Override
    public DB2SparkNodeModel createNodeModel() {
        return new DB2SparkNodeModel();
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<DB2SparkNodeModel> createNodeView(final int viewIndex, final DB2SparkNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new DB2SparkNodeDialog();
    }
}
