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
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.PMMLAssignJob;
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
    private boolean m_transformation;

    /**
     * Constructor.
     */
    public PMMLAssignTask() {
        this(false);
    }

    /**
     * @param isTransformation <code>true</code> if the assignment is based on a PMML transformation
     */
    public PMMLAssignTask(final boolean isTransformation) {
        m_transformation = isTransformation;
    }

    private String predictorDef(final String inputID, final Integer[] colIdxs,
        final Map<String,byte[]> bytecode, final boolean appendProbabilities, final String mainClass, final String outputID) throws GenericKnimeSparkException {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{
                KnimeSparkJob.PARAM_INPUT_TABLE, inputID,
                    ParameterConstants.PARAM_MODEL_NAME, JobConfig.encodeToBase64((Serializable)bytecode),
                    ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])colIdxs),
                    PMMLAssignJob.PARAM_APPEND_PROBABILITIES, Boolean.toString(appendProbabilities),
                    PMMLAssignJob.PARAM_IS_TRANSFORMATION, Boolean.toString(m_transformation),
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
        final String predictorParams = predictorDef(inputRDD.getID(), colIdxs, pmml.getBytecode(), appendProbabilities,
            pmml.getModelClassName(), resultRDD.getID());
        KNIMESparkContext context = inputRDD.getContext();
        exec.checkCanceled();
        JobControler.startJobAndWaitForResult(context, PMMLAssignJob.class.getCanonicalName(), predictorParams, exec);
    }
}
