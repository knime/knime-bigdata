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
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.Predictor;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.data.SparkDataTable;

/**
 *
 * @author dwk
 */
public class AssignTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private String kmeansPredictorDef(final KMeansModel aModel, final String aInputTableName,
        final Integer[] colIdxs, final String aOutputTableName) throws GenericKnimeSparkException {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{ParameterConstants.PARAM_MODEL_NAME, JobConfig.encodeToBase64(aModel),
                ParameterConstants.PARAM_TABLE_1, aInputTableName,
                ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])colIdxs)}, ParameterConstants.PARAM_OUTPUT,
            new String[]{ParameterConstants.PARAM_TABLE_1, aOutputTableName}});
    }
    /**
     * @param exec
     * @param data
     * @param model
     * @param integers
     * @param resultRDD
     */
    void execute(final ExecutionMonitor exec, final SparkDataTable inputRDD, final KMeansModel model,
        final Integer[] colIdxs, final SparkDataTable resultRDD) throws GenericKnimeSparkException, CanceledExecutionException {
        final String predictorKMeansParams = kmeansPredictorDef(model, inputRDD.getID(), colIdxs, resultRDD.getID());
        exec.checkCanceled();
        final String jobId = JobControler.startJob(inputRDD.getContext(), Predictor.class.getCanonicalName(),
            predictorKMeansParams);

        assert (JobControler.waitForJob(inputRDD.getContext(), jobId, exec) != JobStatus.UNKNOWN); //, "job should have finished properly");

        assert (JobStatus.OK != JobControler.getJobStatus(inputRDD.getContext(), jobId)); //"job should not be running anymore",

    }


}
