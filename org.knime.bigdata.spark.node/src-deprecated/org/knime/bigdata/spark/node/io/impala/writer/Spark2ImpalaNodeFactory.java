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
 *   Created on 27.05.2015 by koetter
 */
package org.knime.bigdata.spark.node.io.impala.writer;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveNodeDialog;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@Deprecated
public class Spark2ImpalaNodeFactory extends DefaultSparkNodeFactory<Spark2ImpalaNodeModel> {

    private static final FileFormat[] m_fileFormats = FileFormat.getImpalaFormats();

    /**
     * Creates a Spark2Impala NodeFactory
     */
    public Spark2ImpalaNodeFactory() {
        super("io/db");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Spark2ImpalaNodeModel createNodeModel() {
        return new Spark2ImpalaNodeModel();
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
    public NodeView<Spark2ImpalaNodeModel> createNodeView(final int viewIndex, final Spark2ImpalaNodeModel nodeModel) {
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
        return new Spark2HiveNodeDialog(m_fileFormats);
    }

}
