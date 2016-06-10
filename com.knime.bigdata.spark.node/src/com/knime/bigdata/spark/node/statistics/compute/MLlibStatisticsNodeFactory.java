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
 *   Created on 26.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.statistics.compute;

import org.knime.core.node.NodeDialogPane;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibStatisticsNodeFactory extends DefaultSparkNodeFactory<MLlibStatisticsNodeModel> {

    /**
     * Constructor.
     */
    public MLlibStatisticsNodeFactory() {
        super("statistics");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MLlibStatisticsNodeModel createNodeModel() {
        return new MLlibStatisticsNodeModel();
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
        return new MLlibStatisticsNodeDialog();
    }

}
