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
 */
package org.knime.bigdata.spark.node.io.database.impala.writer;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.node.io.database.hive.writer.DBSpark2HiveNodeDialog;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 * Impala to Spark node factory.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DBSpark2ImpalaNodeFactory extends DefaultSparkNodeFactory<DBSpark2ImpalaNodeModel> {

    private static final FileFormat[] m_fileFormats = FileFormat.getImpalaFormats();

    /**
     * Creates a DBSpark2Impala node factory
     */
    public DBSpark2ImpalaNodeFactory() {
        super("io/db");
    }

    @Override
    public DBSpark2ImpalaNodeModel createNodeModel() {
        return new DBSpark2ImpalaNodeModel();
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<DBSpark2ImpalaNodeModel> createNodeView(final int viewIndex, final DBSpark2ImpalaNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new DBSpark2HiveNodeDialog(m_fileFormats);
    }
}