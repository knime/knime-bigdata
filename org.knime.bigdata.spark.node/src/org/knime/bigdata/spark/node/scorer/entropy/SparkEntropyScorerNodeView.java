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
 *   Created on Sep 30, 2015 by bjoern
 */
package org.knime.bigdata.spark.node.scorer.entropy;

import org.knime.base.node.mine.scorer.entrop.AbstractEntropyNodeView;

/**
 * Node view of the Spark Entropy Scorer node, which displays some quality statistics about a clustering.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkEntropyScorerNodeView extends AbstractEntropyNodeView<SparkEntropyScorerNodeModel> {

    /** Inits GUI.
     * @param model the node model from which of display results.
     */
    public SparkEntropyScorerNodeView(final SparkEntropyScorerNodeModel model) {
        super(model);
    }

    /** {@inheritDoc} */
    @Override
    protected void modelChanged() {
        final SparkEntropyScorerViewData viewData = getNodeModel().getViewData();
        if (viewData != null) {
            getEntropyView().update(viewData.getScoreTable(), viewData.getNrClusters(), viewData.getOverallSize(),
                viewData.getNrReferenceClusters(), viewData.getOverallSize(), viewData.getOverallEntropy(),
                viewData.getOverallQuality());
        }
    }
}
