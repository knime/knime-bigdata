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
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package com.knime.bigdata.spark.node.scorer.accuracy;

import org.knime.base.node.mine.scorer.accuracy.AbstractAccuracyScorerNodeView;
import org.knime.base.node.mine.scorer.accuracy.ScorerViewData;
import org.knime.core.node.NodeView;

/**
 *  * This implements the {@link AbstractAccuracyScorerNodeView} for the {@link SparkAccuracyScorerNodeModel}.
 *
 * @author Ole Ostergaard
 */
final class SparkAccuracyScorerNodeView extends AbstractAccuracyScorerNodeView<SparkAccuracyScorerNodeModel> {



    /**Delegates to super class.
     * Constructs the {@link NodeView} from the given {@link SparkAccuracyScorerNodeModel}.
     * @param nodeModel
     */
    public SparkAccuracyScorerNodeView(final SparkAccuracyScorerNodeModel nodeModel) {
        super(nodeModel, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ScorerViewData getScorerViewData() {
        return getNodeModel().getViewData();
    }
}
