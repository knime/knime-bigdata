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
 *   Created on Sep 30, 2015 by bjoern
 */
package com.knime.bigdata.spark.node.scorer.entropy;

import org.knime.core.data.DataTable;
import org.knime.core.node.BufferedDataTable;

import com.knime.bigdata.spark.jobserver.server.EntropyScorerData;

/**
 *
 * @author bjoern
 */
public class SparkEntropyScorerViewData {

    private final EntropyScorerData m_scoreData;

    private final BufferedDataTable m_scoreTable;

    public SparkEntropyScorerViewData(final EntropyScorerData scoreData, final BufferedDataTable scoreTable) {
        m_scoreData = scoreData;
        m_scoreTable = scoreTable;
    }

    /**
     * @return the scoring data as returned by the Spark job.
     */
    public EntropyScorerData getM_scoreData() {
        return m_scoreData;
    }

    /**
     * @return the scoring data as a data table.
     */
    public BufferedDataTable getM_scoreTable() {
        return m_scoreTable;
    }

    public int getOverallSize() {
        return m_scoreData.getOverallSize();
    }

    public double getOverallEntropy() {
        return m_scoreData.getEntropy();
    }

    public double getOverallNormalizedEntropy() {
        return m_scoreData.getOverallNormalizedEntropy();
    }

   public double getOverallQuality() {
       return m_scoreData.getQuality();
   }

   public DataTable getScoreTable() {
       return m_scoreTable;
   }

    /**
     * @return number of cluster found in data
     */
    public int getNrClusters() {
        return m_scoreData.getNrClusters();
    }

    /**
     * @return
     */
    public int getOverallSizeOfClusters() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * @return
     */
    public int getNrReferenceClusters() {
        return m_scoreData.getNrReferenceClusters();
    }
}
