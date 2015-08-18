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
package com.knime.bigdata.spark.node.mllib;

import java.io.Serializable;

import javax.annotation.Nullable;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.SVDJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author koetter
 */
public class SVDTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer[] m_numericColIdx;

    private final boolean m_computeU;

    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final String m_UMatrixName;

    private final String m_VMatrixName;

    private final int m_k;

    private final double m_rCond;

    SVDTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final boolean aComputeU, final int aK,
        final double aRCond, final String aVMatrix, @Nullable final String aUMatrix) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, aComputeU, aK, aRCond, aVMatrix, aUMatrix);
    }

    SVDTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] featureColIdxs, final boolean aComputeU, final int aK,
        final double aRCond, final String aVMatrix, @Nullable final String aUMatrix) {
        m_k = aK;
        m_computeU = aComputeU;
        m_UMatrixName = m_computeU ? aUMatrix : null;
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_numericColIdx = featureColIdxs;
        m_rCond = aRCond;
        m_VMatrixName = aVMatrix;
    }

    double[] execute(final ExecutionMonitor exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String learnerParams = paramsAsJason();
        if (exec != null) {
            exec.checkCanceled();
        }
        final String jobId = JobControler.startJob(m_context, SVDJob.class.getCanonicalName(), learnerParams);
        final JobResult result = JobControler.waitForJobAndFetchResult(m_context, jobId, exec);

        return (double[])result.getObjectResult();
    }

    String paramsAsJason() {
        return paramsAsJason(m_inputTableName, m_numericColIdx, m_computeU, m_k, m_rCond, m_VMatrixName, m_UMatrixName);
    }

    /**
     * (non-private for unit testing)
     *
     * @param aInputTableName
     * @param aNumericColIdx
     * @param aComputeU
     * @param aK
     * @param aRCond
     * @param aVMatrix
     * @param aUMatrix
     * @return Json representation of parameters
     */
    static String paramsAsJason(final String aInputTableName, final Integer[] aNumericColIdx, final boolean aComputeU,
        final int aK, final double aRCond, final String aVMatrix, @Nullable final String aUMatrix) {

        final Object[] inputParamas =
            new Object[]{SVDJob.PARAM_COMPUTE_U, aComputeU, SVDJob.PARAM_K, aK, SVDJob.PARAM_RCOND, aRCond,
                ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])aNumericColIdx),
                KnimeSparkJob.PARAM_INPUT_TABLE, aInputTableName};

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT,
            new String[]{SVDJob.PARAM_RESULT_MATRIX_V, aVMatrix, SVDJob.PARAM_RESULT_MATRIX_U, aUMatrix}});
    }
}
