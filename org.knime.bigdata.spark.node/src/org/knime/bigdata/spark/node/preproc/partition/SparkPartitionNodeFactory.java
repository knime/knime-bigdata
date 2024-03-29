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
package org.knime.bigdata.spark.node.preproc.partition;

import org.knime.base.node.preproc.sample.SamplingNodeSettings;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.CountMethods;
import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.node.preproc.sampling.SparkSamplingNodeDialog;
import org.knime.core.node.NodeDialogPane;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPartitionNodeFactory extends DefaultSparkNodeFactory<SparkPartitionNodeModel> {

    /**
     * Constructor.
     */
    public SparkPartitionNodeFactory() {
        super("row");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkPartitionNodeModel createNodeModel() {
        return new SparkPartitionNodeModel();
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
        final SparkSamplingNodeDialog sparkSamplingNodeDialog = new SparkSamplingNodeDialog();
        final SamplingNodeSettings settings = sparkSamplingNodeDialog.getSamplingSettings();
        settings.setDefaultCountMethod(CountMethods.Relative);
        settings.setDefaultFraction(0.7);
        return sparkSamplingNodeDialog;
    }

}
