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

import javax.json.JsonObject;

import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.JavaRDDFromFile;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;


/**
 *
 * @author koetter
 */
public class FileToRDDTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String m_inputTableName;

    /**
     * constructor - simply stores parameters
     *
     * @param aInputTableName - table identifier (input data and key for output)
     */
    public FileToRDDTask(final String aInputTableName) {
        m_inputTableName = aInputTableName;
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
        final String params = text2RDDDef(m_inputTableName);

        String jobId = JobControler.startJob(contextName, JavaRDDFromFile.class.getCanonicalName(), params);

        assert(JobControler.waitForJob(jobId, exec) != JobStatus.UNKNOWN); //job should have finished properly

        assert (JobStatus.OK != JobControler.getJobStatus(jobId)); //job should not be running anymore

        JsonObject statusWithResult = JobControler.fetchJobResult(jobId);
        assert (statusWithResult != null);
        assert ("OK".equals(statusWithResult.getString("status"))); //should return OK as result status

        return statusWithResult.getString("result");
    }

    private final String text2RDDDef(final String aFileName) {
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_DATA_PATH, aFileName},
            //yes, same fileName - for the output this is just a key
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_DATA_PATH, aFileName}});
    }

}
