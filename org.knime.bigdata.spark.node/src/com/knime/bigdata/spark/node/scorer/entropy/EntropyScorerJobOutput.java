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
 *   Created on May 13, 2016 by oole
 */
package com.knime.bigdata.spark.node.scorer.entropy;

import java.util.List;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.scorer.entropy.EntropyScorerData.ClusterScore;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class EntropyScorerJobOutput extends JobOutput {

    private static final String CLUSTER_SCORES = "clusterScores";
    private static final String OVERALL_ENTROPY = "overallEntropy";
    private static final String OVERALL_NORM_ENTROPY = "overallNormalizedEntropy";
    private static final String OVERALL_QUALITY = "overallQuality";
    private static final String OVERALL_SIZE = "overallSize";
    private static final String NR_CLUSTERS = "nrClusters";
    private static final String NR_REFERENCE_CLUSTERS = "nrReferenceClusters";
    /**
     * Paramless constructor for automatic deserialization
     */

    public EntropyScorerJobOutput() {}

    /**
     * @param clusterScores
     * @param overallEntropy
     * @param overallNormalizedEntropy
     * @param overallQuality
     * @param overallSize
     * @param nrClusters
     * @param nrReferenceClusters
     */
    public EntropyScorerJobOutput(final List<ClusterScore> clusterScores, final double overallEntropy,
        final double overallNormalizedEntropy, final double overallQuality, final int overallSize, final int nrClusters,
        final int nrReferenceClusters) {
        set(CLUSTER_SCORES, clusterScores);
        set(OVERALL_ENTROPY, overallEntropy);
        set(OVERALL_NORM_ENTROPY, overallNormalizedEntropy);
        set(OVERALL_QUALITY, overallQuality);
        set(OVERALL_SIZE, overallSize);
        set(NR_CLUSTERS, nrClusters);
        set(NR_REFERENCE_CLUSTERS, nrReferenceClusters);
    }

    /**
     * @return the cluster scores
     */
    public List<ClusterScore> getClusterScores() {
        return get(CLUSTER_SCORES);
    }

    /**
     * @return the overall entropy
     */
    public double getOverallEntropy() {
        return getDouble(OVERALL_ENTROPY);
    }

    /**
     * @return the overall normalized entropy
     */
    public double getOverallNormalizedEntropy() {
        return getDouble(OVERALL_NORM_ENTROPY);
    }

    /**
     * @return the quality
     */
    public double getOverallQuality() {
        return getDouble(OVERALL_QUALITY);
    }

    /**
     * @return the overall size
     */
    public int getOverallSize() {
        return getInteger(OVERALL_SIZE);
    }

    /**
     * @return the number of clsuters
     */
    public int getNrClusters() {
        return getInteger(NR_CLUSTERS);
    }

    /**
     * @return the number of reference clusters
     */
    public int getNrReferenceClusters() {
        return getInteger(NR_REFERENCE_CLUSTERS);
    }



}
