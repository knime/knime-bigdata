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
package com.knime.bigdata.spark.node.mllib.clustering.assigner;

import java.io.Serializable;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.KMeansPredictor;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;

/**
 *
 * @author dwk
 */
public class AssignTask implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private String kmeansPredictorDef(final KMeansModel aModel, final String aInputTableName,
        final String aOutputTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_MODEL_NAME, ModelUtils.toString(aModel),
                ParameterConstants.PARAM_TABLE_1, aInputTableName}, ParameterConstants.PARAM_OUTPUT,
            new String[]{ParameterConstants.PARAM_TABLE_1, aOutputTableName}});
    }

    void execute(final String contextName, final ExecutionContext exec, final String aTableName,
        final KMeansModel aModel, final String aOutputTableName) throws GenericKnimeSparkException, CanceledExecutionException {
        final String predictorKMeansParams = kmeansPredictorDef(aModel, aTableName, aOutputTableName);

        String jobId =
            JobControler.startJob(contextName, KMeansPredictor.class.getCanonicalName(), predictorKMeansParams);

        assert (JobControler.waitForJob(jobId, exec) != JobStatus.UNKNOWN); //, "job should have finished properly");

        assert (JobStatus.OK != JobControler.getJobStatus(jobId)); //"job should not be running anymore",

    }

}
