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
package com.knime.bigdata.spark.node.io.hive.reader;

import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.HiveToRDDJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.data.SparkRDD;


/**
 *
 * @author dwk, jfr
 */
public class HiveToRDDTask {

    private final SparkRDD m_rdd;

    private final String m_hiveQuery;

    /**
     * constructor - simply stores parameters
     * @param rdd - the {@link SparkRDD} to generate
     * @param hiveQuery - the hive query to execute
     */
    public HiveToRDDTask(final SparkRDD rdd, final String hiveQuery) {
        m_rdd = rdd;
        m_hiveQuery = hiveQuery;
    }

    /**
     * run the job on the server
     * @param exec execution context
     * @throws Exception if anything goes wrong
     */
    public void execute(final ExecutionContext exec) throws Exception {
        final String jsonArgs = params2Json();

        String jobId = JobControler.startJob(m_rdd.getContext().getContextName(), HiveToRDDJob.class.getCanonicalName(), jsonArgs);
        JobResult result = JobControler.waitForJobAndFetchResult(jobId, exec);
        assert(m_rdd.getID().equals(result.getFirstTableKey()));
    }

    private final String params2Json() {
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_SQL_STATEMENT, m_hiveQuery},
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, m_rdd.getID()}});
    }

}
