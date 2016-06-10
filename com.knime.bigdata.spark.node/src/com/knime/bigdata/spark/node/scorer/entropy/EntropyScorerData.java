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
 *   Created on 22.09.2015 by dwk
 */
package com.knime.bigdata.spark.node.scorer.entropy;

import java.io.Serializable;
import java.util.List;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * Utility class that stores some entropy and quality values for clustering results given a reference clustering.
 *
 * @author Bernd Wiswedel, University of Konstanz, dwk
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class EntropyScorerData implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<ClusterScore> m_clusterScores;

    private final double m_overallEntropy;

    private final double m_overallNormalizedEntropy;

    private final double m_overallQuality;

    private final int m_overallSize;

    private final int m_nrClusters;

    private final int m_nrReferenceClusters;

    /**
     * Utility class that only holds entropy scores and some other values for a single cluster.
     *
     * @author Bjoern Lohrmann, KNIME.com
     */
    public static class ClusterScore implements Serializable {

        private static final long serialVersionUID = -5155148042602936584L;

        private final Object m_cluster;

        private final int m_size;

        private final double m_entropy;

        private final double m_normalizedEntropy;

        /**
         *
         * @param cluster The cluster identifier (label).
         * @param size The number of objects assigned to the cluster.
         * @param entropy The entropy of the classification w.r.t to reference clustering.
         * @param normalizedEntropy The normalized entropy of the classification.
         */
        public ClusterScore(final Object cluster, final int size, final double entropy,
            final double normalizedEntropy) {
            this.m_cluster = cluster;
            this.m_size = size;
            this.m_entropy = entropy;
            this.m_normalizedEntropy = normalizedEntropy;
        }

        /**
         * @return The cluster identifier (label).
         */
        public Object getCluster() {
            return m_cluster;
        }

        /**
         * @return The number of objects assigned to the cluster.
         */
        public int getSize() {
            return m_size;
        }

        /**
         * @return  The entropy of the classification w.r.t to reference clustering.
         */
        public double getEntropy() {
            return m_entropy;
        }

        /**
         * @return The entropy of the classification normalized to the [0,1] range.
         */
        public double getNormalizedEntropy() {
            return m_normalizedEntropy;
        }
    }

    /**
     *
     * @param clusterScores A list that holds scoring information for each cluster found.
     * @param overallEntropy The overall entropy of the clustering.
     * @param overallNormalizedEntropy The overall normalized entropy of the clustering.
     * @param overallQuality The quality of the clustering (see  {@link #getOverallQuality() })
     * @param overallSize The total number of objects in the clustering.
     * @param nrClusters The number of clusters found.
     * @param nrReferenceClusters The number of reference clusters.
     */
    public EntropyScorerData(final List<ClusterScore> clusterScores, final double overallEntropy,
        final double overallNormalizedEntropy, final double overallQuality, final int overallSize, final int nrClusters,
        final int nrReferenceClusters) {

        m_clusterScores = clusterScores;
        m_overallEntropy = overallEntropy;
        m_overallNormalizedEntropy = overallNormalizedEntropy;
        m_overallQuality = overallQuality;
        m_overallSize = overallSize;
        m_nrClusters = nrClusters;
        m_nrReferenceClusters = nrReferenceClusters;
    }

    /**
     * @return the overall entropy for a number of clusters. The entropy value is not normalized, i.e. the result is in
     *         the range of <code>[0, log<sub>2</sub>(|cluster|)</code>.
     */
    public double getOverallEntropy() {
        return m_overallEntropy;
    }

    /**
     * Get quality measure of current clustering result (in 0-1). The quality value is defined as
     * <p>
     * sum over all clusters (current_cluster_size / patterns_count * (1 - entropy (current_cluster wrt. reference).
     * <p>
     * For further details see Bernd Wiswedel, Michael R. Berthold, <b>Fuzzy Clustering in Parallel Universes</b>,
     * <i>International Journal of Approximate Reasoning</i>, 2006.
     *
     * @return quality value in [0,1]
     */
    public double getOverallQuality() {
        return m_overallQuality;
    }

    /**
     * @return the number of clusters found.
     */
    public int getNrClusters() {
        return m_nrClusters;
    }

    /**
     * @return the number of reference clusters.
     */
    public int getNrReferenceClusters() {
        return m_nrReferenceClusters;
    }

    /**
     *
     * @return the overall (total) number of objects (i.e. size) in all clusters
     */
    public int getOverallSize() {
        return m_overallSize;
    }

    /**
     * @return the overall normalized entropy
     */
    public double getOverallNormalizedEntropy() {
        return m_overallNormalizedEntropy;
    }

    /**
     * @return a list of scoring information for each cluster found.
     */
    public List<ClusterScore> getClusterScores() {
        return m_clusterScores;
    }
}
