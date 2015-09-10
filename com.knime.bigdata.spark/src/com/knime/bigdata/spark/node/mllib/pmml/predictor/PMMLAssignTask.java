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

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.UploadUtil;
import com.knime.bigdata.spark.jobserver.jobs.PMMLAssign;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLAssignTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private String predictorDef(final String inputID, final Integer[] colIdxs,
        final String aTmpFileName, final boolean appendProbabilities, final String mainClass, final String outputID) throws GenericKnimeSparkException {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{
                KnimeSparkJob.PARAM_INPUT_TABLE, inputID,
                    ParameterConstants.PARAM_MODEL_NAME, JobConfig.encodeToBase64(aTmpFileName),
                    ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])colIdxs),
                    PMMLAssign.PARAM_APPEND_PROBABILITIES, Boolean.toString(appendProbabilities),
                    //we have to replace the . with / since the key in the map uses / instead of .
                    ParameterConstants.PARAM_MAIN_CLASS, mainClass.replace('.', '/')},
                ParameterConstants.PARAM_OUTPUT,
                    new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, outputID}});
    }

    /**
     * @param exec
     * @param inputRDD
     * @param pmml
     * @param colIdxs
     * @param appendProbabilities
     * @param resultRDD
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public void execute(final ExecutionMonitor exec, final SparkDataTable inputRDD,
        final CompiledModelPortObject pmml, final Integer[] colIdxs,
        final boolean appendProbabilities, final SparkDataTable resultRDD)
                throws GenericKnimeSparkException, CanceledExecutionException {
        KNIMESparkContext context = inputRDD.getContext();

        final UploadUtil util = new UploadUtil(context, (Serializable)pmml.getBytecode(), "bytecode");
        util.upload();
        try {
            final String predictorParams = predictorDef(inputRDD.getID(), colIdxs, util.getServerFileName(), appendProbabilities,
                pmml.getModelClassName(), resultRDD.getID());
            exec.checkCanceled();
            JobControler.startJobAndWaitForResult(context, PMMLAssign.class.getCanonicalName(), predictorParams, exec);
        } finally {
            util.cleanup();
        }
    }
}
