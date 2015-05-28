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

import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.HiveToRDDJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;


/**
 *
 * @author dwk, jfr
 */
public class HiveToRDDTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String m_tableName;

    private final String m_hiveQuery;

    /**
     * constructor - simply stores parameters
     * @param tableName - table identifier the key for the output table
     * @param hiveQuery - the hive query to execute
     */
    public HiveToRDDTask(final String tableName, final String hiveQuery) {
        m_tableName = tableName;
        m_hiveQuery = hiveQuery;
    }

    /**
     * run the job on the server
     * @param exec execution context
     *
     * @return name of RDD
     * @throws Exception if anything goes wrong
     */
    public String execute(final ExecutionContext exec) throws Exception {

        final String contextName = KnimeContext.getSparkContext();
        final String jsonArgs = params2Json();

        String jobId = JobControler.startJob(contextName, HiveToRDDJob.class.getCanonicalName(), jsonArgs);
        JobResult result = JobControler.waitForJobAndFetchResult(jobId, exec);
        return result.getFirstTableKey();
    }

    private final String params2Json() {
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_SQL_STATEMENT, m_hiveQuery},
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_DATA_PATH, m_tableName}});
    }

}
