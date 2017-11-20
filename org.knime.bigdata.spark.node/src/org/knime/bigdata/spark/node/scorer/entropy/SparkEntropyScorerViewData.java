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

import org.knime.core.node.BufferedDataTable;

/**
 * Utility class that holds entropy scoring information to be displayed by the node view.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkEntropyScorerViewData {

    private final EntropyScorerJobOutput     m_scoreData;

    private final BufferedDataTable m_scoreTable;

    /**
     *
     * @param outPut The raw scoring data to display.
     * @param scoreTable The scoring data as a data table (which is also forward by the scorer node).
     */
    public SparkEntropyScorerViewData(final EntropyScorerJobOutput outPut, final BufferedDataTable scoreTable) {
        m_scoreData = outPut;
        m_scoreTable = scoreTable;
    }

    /**
     * @return the scoring data as returned by the Spark job.
     */
    public EntropyScorerJobOutput getEntropyScorerData() {
        return m_scoreData;
    }

    /**
     * @return the scoring data as a data table.
     */
    public BufferedDataTable getScoreTable() {
        return m_scoreTable;
    }

    /**
     * @return {@link EntropyScorerData#getOverallSize()}
     */
    public int getOverallSize() {
        return m_scoreData.getOverallSize();
    }

    /**
     * @return {@link EntropyScorerData#getOverallEntropy()}
     */
    public double getOverallEntropy() {
        return m_scoreData.getOverallEntropy();
    }

    /**
     * @return {@link EntropyScorerData#getOverallNormalizedEntropy()}
     */
    public double getOverallNormalizedEntropy() {
        return m_scoreData.getOverallNormalizedEntropy();
    }

    /**
     * @return {@link EntropyScorerData#getOverallQuality()}
     */
    public double getOverallQuality() {
        return m_scoreData.getOverallQuality();
    }

    /**
     * @return {@link EntropyScorerData#getNrClusters()}
     */
    public int getNrClusters() {
        return m_scoreData.getNrClusters();
    }

    /**
     * @return {@link EntropyScorerData#getNrReferenceClusters()}
     */
    public int getNrReferenceClusters() {
        return m_scoreData.getNrReferenceClusters();
    }
}
