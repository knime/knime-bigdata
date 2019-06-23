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
 *   Created on Jun 13, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * Strategy to determine features to consider for model training. Supported: "auto", "all", "sqrt",
 * "log2", "onethird". If "auto" is set, parameter is set based on numTrees: if numTrees == 1, set to "all"; if
 * numTrees > 1 (forest) set to "sqrt".
 */
public enum FeatureSamplingStrategy implements ButtonGroupEnumInterface {
    /**  */
    auto("Auto"),
    /**  */
    all("All"),
    /**  */
    sqrt("Square root"),
    /**  */
    log2("Log2"),
    /**  */
    onethird("One third");

    private final String m_text;

    private FeatureSamplingStrategy(final String text) {
        m_text = text;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getText() {
        return m_text;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getActionCommand() {
        return name();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getToolTip() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDefault() {
        return this == auto;
    }
}