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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.EntropyScorerData;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * computes cluster some entropy and quality values for clustering results given a reference clustering.
 *
 * @author dwk
 */
public class EntropyScorerJob extends KnimeSparkJob implements Serializable {

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

    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_ACTUAL_COL_INDEX)) {
            msg = "Input parameter '" + PARAM_ACTUAL_COL_INDEX + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_PREDICTION_COL_INDEX)) {
            msg = "Input parameter '" + PARAM_PREDICTION_COL_INDEX + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(KnimeSparkJob.PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + KnimeSparkJob.PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }

        return ValidationResultConverter.valid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JobResult runJobWithContext(final SparkContext aSparkContext, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, getLogger());
        getLogger().log(Level.INFO, "START " + getAlgName() + " job...");

        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));

        final Integer referenceCol = aConfig.getInputParameter(PARAM_ACTUAL_COL_INDEX, Integer.class);
        final Integer clusterCol = aConfig.getInputParameter(PARAM_PREDICTION_COL_INDEX, Integer.class);
        final Serializable scores = scoreCluster(rowRDD, referenceCol, clusterCol);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(scores);

        getLogger().log(Level.INFO, "DONE " + getAlgName() + " job...");
        return res;
    }

    static Serializable scoreCluster(final JavaRDD<Row> aRowRDD, final Integer referenceCol, final Integer clusterCol) {

        final Map<Integer, Map<Integer, Integer>> emptyMap = new HashMap<>();

        //for each (predicted) cluster, we collect the distribution of its members over the reference clusters
        // clusterId -> (referenceClusterId -> count)
        final Map<Integer, Map<Integer, Integer>> clusters =
            aRowRDD
                .aggregate(
                    emptyMap,
                    new Function2<Map<Integer, Map<Integer, Integer>>, Row, Map<Integer, Map<Integer, Integer>>>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Map<Integer, Map<Integer, Integer>> call(
                            final Map<Integer, Map<Integer, Integer>> aAggregatedValues, final Row aRow)
                            throws Exception {
                            final Integer cluster = (int)RDDUtils.getDouble(aRow, clusterCol);
                            final Integer reference = (int)RDDUtils.getDouble(aRow, referenceCol);
                            final Map<Integer, Integer> values;
                            if (!aAggregatedValues.containsKey(cluster)) {
                                values = new HashMap<>();
                                aAggregatedValues.put(cluster, values);
                            } else {
                                values = aAggregatedValues.get(cluster);
                            }
                            if (!values.containsKey(reference)) {
                                values.put(reference, 1);
                            } else {
                                values.put(reference, values.get(reference) + 1);
                            }
                            return aAggregatedValues;
                        }
                    },
                    new Function2<Map<Integer, Map<Integer, Integer>>, Map<Integer, Map<Integer, Integer>>, Map<Integer, Map<Integer, Integer>>>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Map<Integer, Map<Integer, Integer>> call(
                            final Map<Integer, Map<Integer, Integer>> aAggregatedValues0,
                            final Map<Integer, Map<Integer, Integer>> aAggregatedValues1) throws Exception {
                            for (Map.Entry<Integer, Map<Integer, Integer>> entry : aAggregatedValues0.entrySet()) {
                                final Integer key = entry.getKey();
                                final Map<Integer, Integer> aggregatedCounts = entry.getValue();
                                if (aAggregatedValues1.containsKey(key)) {
                                    Map<Integer, Integer> values = aAggregatedValues1.remove(key);
                                    for (Map.Entry<Integer, Integer> counts : aggregatedCounts.entrySet()) {
                                        if (values.containsKey(counts.getKey())) {
                                            final Integer count = values.remove(counts.getKey());
                                            aggregatedCounts.put(counts.getKey(), counts.getValue() + count);
                                        }
                                    }
                                    for (Map.Entry<Integer, Integer> counts : values.entrySet()) {
                                        aggregatedCounts.put(counts.getKey(), counts.getValue());
                                    }
                                }
                            }
                            for (Map.Entry<Integer, Map<Integer, Integer>> entry : aAggregatedValues1.entrySet()) {
                                aAggregatedValues0.put(entry.getKey(), entry.getValue());
                            }
                            return aAggregatedValues0;
                        }
                    });

        // get the number of different clusters in the reference set
        final Set<Integer> reference = new HashSet<>();
        for (Map<Integer, Integer> pats : clusters.values()) {
            reference.addAll(pats.keySet());
        }
        final int refClusterCount = reference.size();

        final double entropy = entropy(clusters);
        final double quality = quality(clusters, refClusterCount);
        final int clusterCount = clusters.size();

        return new EntropyScorerData(entropy, quality, clusterCount, refClusterCount);
    }

    /**
     * Get entropy for a number of clusters, the entropy value is not normalized, i.e. the result is in the range of
     * <code>[0, log<sub>2</sub>(|cluster|)</code>.
     *
     *
     * @param clusters the clusters to score
     * @return entropy value
     */
    static double entropy(final Map<Integer, Map<Integer, Integer>> clusters) {
        if (clusters.isEmpty()) {
            return 0.0;
        }
        double entropy = 0.0;
        int patCount = 0;
        for (Map<Integer, Integer> cluster : clusters.values()) {
            final int size = computeClusterSize(cluster);
            patCount += size;
            final double e = clusterEntropy(cluster, size);
            entropy += size * e;
        }
        // normalizing over the number of objects in the reference set
        return entropy / patCount;
    }

    /**
     * Get quality measure of current cluster result (in 0-1). The quality value is defined as
     * <p>
     * sum over all clusters (curren_cluster_size / patterns_count * (1 - entropy (current_cluster wrt. reference).
     * <p>
     * For further details see Bernd Wiswedel, Michael R. Berthold, <b>Fuzzy Clustering in Parallel Universes</b>,
     * <i>International Journal of Approximate Reasoning</i>, 2006.
     *
     * @param refClusterCount
     *
     * @param clusters the map containing the clusters that have been found, i.e. clusterID (as above) as key and the
     *            set of all contained patterns as value
     * @return quality value in [0,1]
     */
    static double quality(final Map<Integer, Map<Integer, Integer>> clusters, final int refClusterCount) {
        // optimistic guess (we don't have counterexamples!)
        if (clusters.isEmpty()) {
            return 1.0;
        }

        // normalizing value (such that the maximum value for the entropy is 1
        double normalizer = Math.log(refClusterCount) / Math.log(2.0);
        double quality = 0.0;
        int patCount = 0;
        //use group by predicted cluster, then compute entropy for each group in this loop
        for (Map<Integer, Integer> pats : clusters.values()) {
            int size = computeClusterSize(pats);
            patCount += size;
            double entropy = clusterEntropy(pats, size);
            double normalizedEntropy;
            if (normalizer == 0.0) {
                assert entropy == 0.0;
                normalizedEntropy = 0.0;
            } else {
                normalizedEntropy = entropy / normalizer;
            }
            quality += size * (1.0 - normalizedEntropy);
        }
        // normalizing over the number of objects in the reference set
        return quality / patCount;
    }

    /**
     * Get entropy for one single cluster.
     *
     * @param cluster the single cluster to score
     * @return the (not-normalized) entropy of <code>pats</code> wrt. <code>ref</code>
     */
    static double clusterEntropy(final Map<Integer, Integer> cluster, final int size) {

        double e = 0.0;
        for (Integer clusterCount : cluster.values()) {
            int count = clusterCount.intValue();
            double quot = count / (double)size;
            e -= quot * Math.log(quot) / Math.log(2.0);
        }
        return e;
    }

    /**
     * @param cluster
     * @return
     */
    private static int computeClusterSize(final Map<Integer, Integer> cluster) {
        int size = 0;
        for (Integer s : cluster.values()) {
            size += s;
        }
        return size;
    }

}
