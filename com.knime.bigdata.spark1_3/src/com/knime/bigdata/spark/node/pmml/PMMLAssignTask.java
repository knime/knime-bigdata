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
package com.knime.bigdata.spark.node.pmml;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.UploadUtil;
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
public abstract class PMMLAssignTask implements Serializable {

    private static final long serialVersionUID = 1L;
    private String m_jobClass;

    /**
     * @param jobClass the cjob class name
     */
    protected PMMLAssignTask(final String jobClass) {
        m_jobClass = jobClass;
    }

    private String predictorDef(final String inputID, final Integer[] colIdxs,
        final String aTmpFileName, final String mainClass, final String outputID) throws GenericKnimeSparkException {
        final Object[] inputParams = new Object[]{
            KnimeSparkJob.PARAM_INPUT_TABLE, inputID,
                ParameterConstants.PARAM_MODEL_NAME, JobConfig.encodeToBase64(aTmpFileName),
                ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])colIdxs),
                //we have to replace the . with / since the key in the map uses / instead of .
                ParameterConstants.PARAM_MAIN_CLASS, mainClass.replace('.', '/')};
        final Object[] additionalInputParams = getAdditionalInputParams();
        final Object[] combinedInput = ArrayUtils.addAll(inputParams, additionalInputParams);
        return JsonUtils.asJson(
            new Object[]{ParameterConstants.PARAM_INPUT, combinedInput,
                ParameterConstants.PARAM_OUTPUT, new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, outputID}});
    }


    /**
     * @return additional input parameters
     */
    protected abstract Object[] getAdditionalInputParams();

    /**
     * @param exec
     * @param data
     * @param pmml
     * @param colIdxs
     * @param resultRDD
     * @throws CanceledExecutionException
     * @throws GenericKnimeSparkException
     */
    public void execute(final ExecutionContext exec, final SparkDataTable data, final CompiledModelPortObject pmml, final Integer[] colIdxs,
        final String resultRDD) throws GenericKnimeSparkException, CanceledExecutionException {
        execute(exec, data, pmml.getBytecode(), pmml.getModelClassName(), colIdxs, resultRDD);
    }

    /**
     * @param exec
     * @param inputRDD
     * @param bytecode
     * @param mainClass
     * @param colIdxs
     * @param resultRDD
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public void execute(final ExecutionMonitor exec, final SparkDataTable inputRDD,
        final Map<String,byte[]> bytecode, final String mainClass, final Integer[] colIdxs, final String resultRDD)
                throws GenericKnimeSparkException, CanceledExecutionException {
        final KNIMESparkContext context = inputRDD.getContext();
        final UploadUtil util = new UploadUtil(context, (Serializable)bytecode, "bytecode-" + resultRDD);
        util.upload();
        try {
            final String predictorParams = predictorDef(inputRDD.getID(), colIdxs, util.getServerFileName(), mainClass, resultRDD);
            exec.checkCanceled();
            JobControler.startJobAndWaitForResult(context, m_jobClass, predictorParams, exec);
        } finally {
            util.cleanup();
        }
    }
}
