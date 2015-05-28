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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.KMeansLearner;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;

/**
 *
 * @author koetter
 */
public class KMeansTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String m_inputTableName;

    private final String m_outputTableName;

    private final int m_noOfIteration;

    private final int m_noOfCluster;

    private int[] m_includeColIdxs;

    /**
     * constructor - simply stores parameters
     *
     * @param aInputTableName - table identifier (input data)
     * @param includeColIdxs - index of the columns to include starting with 0
     * @param noOfCluster - number of clusters (aka "k")
     * @param noOfIteration - maximal number of iterations
     * @param aOutputTableName - table identifier (classified output data)
     */
    public KMeansTask(final String aInputTableName, final int[] includeColIdxs, final int noOfCluster,
        final int noOfIteration, final String aOutputTableName) {
        m_inputTableName = aInputTableName;
        m_includeColIdxs = includeColIdxs;
        m_outputTableName = aOutputTableName;
        m_noOfCluster = noOfCluster;
        m_noOfIteration = noOfIteration;
    }

    /**
     * run the job on the server
     * @param exec
     *
     * @return KMeansModel
     * @throws Exception
     */
    public KMeansModel execute(final ExecutionContext exec) throws Exception {
        final String contextName = KnimeContext.getSparkContext();
        try {
            final String learnerKMeansParams = kmeansLearnerDef();
            final String jobId =
                JobControler.startJob(contextName, KMeansLearner.class.getCanonicalName(), learnerKMeansParams);

            JobResult result = JobControler.waitForJobAndFetchResult(jobId, exec);

            //TODO - we might want to use the schema of the predicted data as well
            return (KMeansModel)result.getObjectResult();
        } catch (Exception e) {

            throw e;
        }
    }

    private String kmeansLearnerDef() {
        //TODO: Add int array to config
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_COL_IDXS, Arrays.toString(m_includeColIdxs),
                ParameterConstants.PARAM_NUM_CLUSTERS, "" + m_noOfCluster,
                ParameterConstants.PARAM_NUM_ITERATIONS, "" + m_noOfIteration, ParameterConstants.PARAM_DATA_PATH,
                m_inputTableName}, ParameterConstants.PARAM_OUTPUT,
            new String[]{ParameterConstants.PARAM_DATA_PATH, m_outputTableName}});
    }

}
