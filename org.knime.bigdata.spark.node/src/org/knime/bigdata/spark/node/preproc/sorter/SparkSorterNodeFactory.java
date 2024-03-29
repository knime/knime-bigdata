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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.preproc.sorter;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;

/**
 *
 * @author koetter
 */
public class SparkSorterNodeFactory extends DefaultSparkNodeFactory<SparkSorterNodeModel> {

    /**
     * Constructor.
     */
    public SparkSorterNodeFactory() {
        super("row");
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public SparkSorterNodeModel createNodeModel() {
        return new SparkSorterNodeModel();
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
        return new SparkSorterNodeDialog(false);
    }

}
