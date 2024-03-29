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
package org.knime.bigdata.spark.node.io.hive.writer;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@Deprecated
public class Spark2HiveNodeFactory extends DefaultSparkNodeFactory<Spark2HiveNodeModel> {

    private static final FileFormat[] m_fileFormats = FileFormat.getHiveFormats();

    /**
     * Creates a Spark2Hive node factory
     */
    public Spark2HiveNodeFactory() {
        super("io/db");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Spark2HiveNodeModel createNodeModel() {
        return new Spark2HiveNodeModel();
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
    public NodeView<Spark2HiveNodeModel> createNodeView(final int viewIndex, final Spark2HiveNodeModel nodeModel) {
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
