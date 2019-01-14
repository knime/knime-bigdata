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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark1_2.jobs.scorer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scorer.entropy.EntropyScorerData.ClusterScore;
import org.knime.bigdata.spark.node.scorer.ScorerJobInput;
import org.knime.bigdata.spark.node.scorer.entropy.EntropyScorerJobOutput;
import org.knime.bigdata.spark1_2.api.NamedObjects;
import org.knime.bigdata.spark1_2.api.RDDUtilsInJava;
import org.knime.bigdata.spark1_2.api.SparkJob;

import scala.Tuple2;

/**
 * computes cluster some entropy and quality values for clustering results given a reference clustering.
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class EntropyScorerJob implements SparkJob<ScorerJobInput, EntropyScorerJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(EntropyScorerJob.class.getName());

    /**
     * index of column with actual (true) class or regression values
     */
    public static final String PARAM_ACTUAL_COL_INDEX = "actualColIndex";

    /**
     * index of column with predicted class or regression values
     */
    public static final String PARAM_PREDICTION_COL_INDEX = "predictionColIndex";

    Logger getLogger() {
        return LOGGER;
    }

    String getAlgName() {
        return "EntropyScorer";
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public EntropyScorerJobOutput  runJob(final SparkContext sparkContext, final ScorerJobInput input, final NamedObjects namedObjects) {
        getLogger().info("START " + getAlgName() + " job...");

        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());

        final Integer referenceCol = input.getActualColIdx();
        final Integer clusterCol = input.getPredictionColIdx();
        EntropyScorerJobOutput jobOutput = scoreCluster(rowRDD, referenceCol, clusterCol);

        getLogger().info("DONE " + getAlgName() + " job...");
        return jobOutput;
    }

    private static EntropyScorerJobOutput scoreCluster(final JavaRDD<Row> aRowRDD, final Integer referenceCol,
        final Integer clusterCol) {
        // maps cluster -> (refCluster -> count)
        final Map<Object, Map<Object, Integer>> counts =
            toMapOfMaps(RDDUtilsInJava.aggregatePairs(aRowRDD, clusterCol, referenceCol));

        final int nrReferenceClusters = computeNrReferenceClusters(counts);
        final List<ClusterScore> clusterScores = computeClusterScores(counts, nrReferenceClusters);

        return computeOverallValuesAndCreateEntropyScorerData(clusterScores, nrReferenceClusters);
    }

    private static EntropyScorerJobOutput computeOverallValuesAndCreateEntropyScorerData(
        final List<ClusterScore> clusterScores, final int nrReferenceClusters) {

        final int nrClusters = clusterScores.size();

        int overallSize = 0;
        double overallEntropy = 0;
        double overallNormalizedEntropy = 0;
        double overallQuality = 0;

        if (clusterScores.isEmpty()) {
            overallEntropy = 0;
            overallNormalizedEntropy = 0;
            // optimistic guess (we don't have counterexamples!)
            overallQuality = 1;
        } else {
            for (ClusterScore clusterScore : clusterScores) {
                overallSize += clusterScore.getSize();
                overallEntropy += clusterScore.getSize() * clusterScore.getEntropy();
                overallNormalizedEntropy += clusterScore.getSize() * clusterScore.getNormalizedEntropy();
                overallQuality += clusterScore.getSize() * (1.0 - clusterScore.getNormalizedEntropy());
            }

            overallQuality /= overallSize;
            overallEntropy /= overallSize;
            overallNormalizedEntropy /= overallSize;
        }

        return new EntropyScorerJobOutput(clusterScores, overallEntropy, overallNormalizedEntropy, overallQuality,
            overallSize, nrClusters, nrReferenceClusters);
    }

    private static int computeNrReferenceClusters(final Map<Object, Map<Object, Integer>> counts) {
        // get the number of different clusters in the reference set
        final Set<Object> reference = new HashSet<>();
        for (Map<Object, Integer> clusterCounts : counts.values()) {
            reference.addAll(clusterCounts.keySet());
        }
        return reference.size();
    }

    private static Map<Object, Map<Object, Integer>>
        toMapOfMaps(final Map<Tuple2<Object, Object>, Integer> mapOfTuples) {

        final Map<Object, Map<Object, Integer>> mapOfMaps = new HashMap<>();
        for (Tuple2<Object, Object> tuple : mapOfTuples.keySet()) {
            Map<Object, Integer> map = mapOfMaps.get(tuple._1);
            if (map == null) {
                map = new HashMap<>();
                mapOfMaps.put(tuple._1, map);
            }

            Integer count = map.get(tuple._2);
            if (count == null) {
                count = 0;
            }
            map.put(tuple._2, count + mapOfTuples.get(tuple));
        }

        return mapOfMaps;
    }

    private static List<ClusterScore> computeClusterScores(final Map<Object, Map<Object, Integer>> counts,
        final int nrReferenceClusters) {

        final List<ClusterScore> clusterScores = new LinkedList<>();

        // normalizing value (such that the maximum value for the entropy is 1
        final double normalizer = Math.log(nrReferenceClusters) / Math.log(2d);

        for (Object cluster : counts.keySet()) {
            Map<Object, Integer> clusterCounts = counts.get(cluster);

            final int clusterSize = computeClusterSize(clusterCounts);
            final double clusterEntropy = clusterEntropy(clusterCounts, clusterSize);
            final double normalizedClusterEntropy = clusterEntropy / normalizer;
            clusterScores.add(new ClusterScore(cluster, clusterSize, clusterEntropy, normalizedClusterEntropy));
        }
        return clusterScores;
    }

    /**
     * Get entropy for one single cluster.
     *
     * @param cluster the single cluster to score
     * @return the (not-normalized) entropy of <code>pats</code> wrt. <code>ref</code>
     */
    static double clusterEntropy(final Map<Object, Integer> cluster, final int size) {

        double e = 0.0;
        for (int clusterCount : cluster.values()) {
            double quot = clusterCount / (double)size;
            e -= quot * Math.log(quot) / Math.log(2.0);
        }
        return e;
    }

    /**
     * Counts the number of objects that were classified as within the same cluster.
     *
     * @param map from _reference_ cluster to number of objects
     * @return number of objects in cluster
     */
    private static int computeClusterSize(final Map<Object, Integer> cluster) {
        int size = 0;
        for (Integer s : cluster.values()) {
            size += s;
        }
        return size;
    }
}
