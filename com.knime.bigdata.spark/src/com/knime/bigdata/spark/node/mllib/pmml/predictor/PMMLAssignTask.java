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
package com.knime.bigdata.spark.node.mllib.pmml.predictor;

import java.io.Serializable;
import java.util.Map;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.PMMLAssign;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.data.SparkDataTable;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLAssignTask implements Serializable {

    private static final long serialVersionUID = 1L;

    PMMLAssignTask() throws InvalidSettingsException {
    }

    private String predictorDef(final String inputID, final Integer[] colIdxs,
        final Map<String,byte[]> bytecode, final boolean appendProbabilities, final String outputID) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{
                    ParameterConstants.PARAM_TABLE_1, inputID,
                    ParameterConstants.PARAM_MODEL_NAME, ModelUtils.toString((Serializable)bytecode),
                    ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray(colIdxs),
                    ParameterConstants.PARAM_APPEND_PROBABILITIES, Boolean.toString(appendProbabilities)},
                ParameterConstants.PARAM_OUTPUT,
                    new String[]{ParameterConstants.PARAM_TABLE_1, outputID}});
    }

    void execute(final ExecutionContext exec, final SparkDataTable inputRDD,
        final Map<String,byte[]> bytecode, final Integer[] colIdxs,
        final boolean appendProbabilities, final SparkDataTable resultRDD)
                throws GenericKnimeSparkException, CanceledExecutionException {
        final String predictorParams =
                predictorDef(inputRDD.getID(), colIdxs, bytecode, appendProbabilities, resultRDD.getID());
        final String jobId = JobControler.startJob(inputRDD.getContext(), PMMLAssign.class.getCanonicalName(),
            predictorParams);

        assert (JobControler.waitForJob(jobId, exec) != JobStatus.UNKNOWN); //, "job should have finished properly");

        assert (JobStatus.OK != JobControler.getJobStatus(jobId)); //"job should not be running anymore",

    }


}
