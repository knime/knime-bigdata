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
 *   Created on 29.04.2019 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.io.database.hive.writer;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class DBSpark2HiveNodeFactory extends DefaultSparkNodeFactory<DBSpark2HiveNodeModel> {

    private static final FileFormat[] m_fileFormats = FileFormat.getHiveFormats();

    /**
     * Creates a DBSpark2Hive node factory
     */
    public DBSpark2HiveNodeFactory() {
        super("io/db");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DBSpark2HiveNodeModel createNodeModel() {
        return new DBSpark2HiveNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<DBSpark2HiveNodeModel> createNodeView(final int viewIndex, final DBSpark2HiveNodeModel nodeModel) {
        return null;
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
        return new DBSpark2HiveNodeDialog(m_fileFormats);
    }

}