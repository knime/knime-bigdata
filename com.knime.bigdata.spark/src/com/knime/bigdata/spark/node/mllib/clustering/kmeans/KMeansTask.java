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
package com.knime.bigdata.spark.node.mllib.clustering.kmeans;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.KMeansLearner;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class KMeansTask {

    private final String m_inputTableName;

    private final String m_outputTableName;

    private final int m_noOfIteration;

    private final int m_noOfCluster;

    private final Integer[] m_includeColIdxs;

    private final String m_contextID;

    /**
     * constructor - simply stores parameters
     * @param inputRDD input RDD
     * @param includeColIdxs - indices of the columns to include starting with 0
     * @param noOfCluster - number of clusters (aka "k")
     * @param noOfIteration - maximal number of iterations
     * @param outputRDD - table identifier (classified output data)
     */
    public KMeansTask(final SparkRDD inputRDD, final int[] includeColIdxs, final int noOfCluster,
        final int noOfIteration, final SparkRDD outputRDD) {
        if (!inputRDD.compatible(outputRDD)) {
            throw new IllegalArgumentException("Incompatible rdds");
        }
        m_contextID = inputRDD.getContext().getContextName();
        m_inputTableName = inputRDD.getID();
        m_includeColIdxs = new Integer[includeColIdxs.length];
        int i = 0;
        for (int value : includeColIdxs) {
            m_includeColIdxs[i++] = Integer.valueOf(value);
        }
        m_outputTableName = outputRDD.getID();
        m_noOfCluster = noOfCluster;
        m_noOfIteration = noOfIteration;
    }

    /**
     * run the job on the server
     *
     * @param exec
     *
     * @return KMeansModel
     * @throws Exception
     */
    public KMeansModel execute(final ExecutionContext exec) throws Exception {
        final String learnerKMeansParams = kmeansLearnerDef();
        final String jobId =
                JobControler.startJob(m_contextID, KMeansLearner.class.getCanonicalName(), learnerKMeansParams);

        final JobResult result = JobControler.waitForJobAndFetchResult(jobId, exec);

        return (KMeansModel)result.getObjectResult();
    }

    private String kmeansLearnerDef() {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray(m_includeColIdxs),
                ParameterConstants.PARAM_NUM_CLUSTERS, "" + m_noOfCluster, ParameterConstants.PARAM_NUM_ITERATIONS,
                "" + m_noOfIteration, ParameterConstants.PARAM_TABLE_1, m_inputTableName},
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, m_outputTableName}});
    }

}
