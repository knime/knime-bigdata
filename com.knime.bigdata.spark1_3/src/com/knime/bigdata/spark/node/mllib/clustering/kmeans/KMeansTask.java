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
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.KMeansLearnerJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
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

    private final KNIMESparkContext m_context;

    /**
     * constructor - simply stores parameters
     *
     * @param inputRDD input RDD
     * @param includeColIdxs - indices of the columns to include starting with 0
     * @param noOfCluster - number of clusters (aka "k")
     * @param noOfIteration - maximal number of iterations
     * @param outputRDD - table identifier (classified output data)
     */
    public KMeansTask(final SparkRDD inputRDD, final Integer[] includeColIdxs, final int noOfCluster,
        final int noOfIteration, final String outputRDD) {
        m_context = inputRDD.getContext();
        m_inputTableName = inputRDD.getID();
        m_includeColIdxs = includeColIdxs;
        m_outputTableName = outputRDD;
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
    public KMeansModel execute(final ExecutionMonitor exec) throws Exception {
        final String learnerKMeansParams = kmeansLearnerDef();
        exec.checkCanceled();
        final JobResult result = JobControler.startJobAndWaitForResult(m_context,
            KMeansLearnerJob.class.getCanonicalName(), learnerKMeansParams, exec);

        return (KMeansModel)result.getObjectResult();
    }

    private String kmeansLearnerDef() {
        return kmeansLearnerDef(m_inputTableName, m_includeColIdxs, m_noOfIteration, m_noOfCluster, m_outputTableName);
    }

    /**
     * (unit testing only)
     * @param aInputTableName
     * @param aIncludeColIdxs
     * @param aNoOfIteration
     * @param aNoOfCluster
     * @param aOutputTableName
     * @return json representation of parameters
     */
    public static String kmeansLearnerDef(final String aInputTableName, final Integer[] aIncludeColIdxs,
        final Integer aNoOfIteration, final Integer aNoOfCluster, final String aOutputTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])aIncludeColIdxs),
                KMeansLearnerJob.PARAM_NUM_CLUSTERS, aNoOfCluster, ParameterConstants.PARAM_NUM_ITERATIONS,
                aNoOfIteration, KnimeSparkJob.PARAM_INPUT_TABLE, aInputTableName}, ParameterConstants.PARAM_OUTPUT,
            new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTableName}});
    }

}