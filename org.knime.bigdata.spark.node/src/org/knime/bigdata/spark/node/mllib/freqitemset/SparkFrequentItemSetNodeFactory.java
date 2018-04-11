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
 *   Created on Jan 30, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.freqitemset;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;

/**
 * Frequent item sets node factory.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkFrequentItemSetNodeFactory extends DefaultSparkNodeFactory<SparkFrequentItemSetNodeModel> {

    /** Default constructor. */
    public SparkFrequentItemSetNodeFactory() {
        super("mining/freq-item-sets-association-rules");
    }

    @Override
    public SparkFrequentItemSetNodeModel createNodeModel() {
        return new SparkFrequentItemSetNodeModel();
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new SparkFrequentItemSetNodeDialog();
    }
}
