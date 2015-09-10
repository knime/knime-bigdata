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
package com.knime.bigdata.spark.node.mllib.reduction.pca;

import java.io.Serializable;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.PCAJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author koetter
 */
public class PCATask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer[] m_numericColIdx;

    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final String m_MatrixName;

    private final String m_ProjectionMatrixName;

    private final int m_k;

    PCATask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final int aK, final String aMatrix, final String aProjectionMatrixName) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, aK, aMatrix, aProjectionMatrixName);
    }

    PCATask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] featureColIdxs, final int aK,
        final String aMatrix, final String aProjectionMatrixName) {
        m_k = aK;
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_numericColIdx = featureColIdxs;
        m_MatrixName = aMatrix;
        m_ProjectionMatrixName = aProjectionMatrixName;
    }

    void execute(final ExecutionMonitor exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String learnerParams = paramsAsJason();
        if (exec != null) {
            exec.checkCanceled();
        }
        JobControler.startJobAndWaitForResult(m_context, PCAJob.class.getCanonicalName(),
            learnerParams, exec);

        //return convertColumMajorArrayTo2Dim((double[])result.getObjectResult(), m_k);
    }

//    static double[][] convertColumMajorArrayTo2Dim(final double[] aValues, final int aNCols) {
//        final int nRows = aValues.length / aNCols;
//        final double[][] res = new double[nRows][];
//        for (int i = 0; i < nRows; i++) {
//            res[i] = new double[aNCols];
//            for (int j = 0; j < aNCols; j++) {
//                res[i][j] = aValues[j * nRows + i];
//            }
//        }
//        return res;
//    }

    String paramsAsJason() {
        return paramsAsJason(m_inputTableName, m_numericColIdx, m_k, m_MatrixName, m_ProjectionMatrixName);
    }

    /**
     * (non-private for unit testing)
     *
     * @param aInputTableName
     * @param aNumericColIdx
     * @param aK
     * @param aMatrix
     * @return Json representation of parameters
     */
    static String paramsAsJason(final String aInputTableName, final Integer[] aNumericColIdx, final int aK,
        final String aMatrix, final String aProjectionMatrix) {

        final Object[] inputParamas =
            new Object[]{PCAJob.PARAM_K, aK, ParameterConstants.PARAM_COL_IDXS,
                JsonUtils.toJsonArray((Object[])aNumericColIdx), KnimeSparkJob.PARAM_INPUT_TABLE, aInputTableName};

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{PCAJob.PARAM_RESULT_MATRIX, aMatrix, PCAJob.PARAM_RESULT_PROJECTION, aProjectionMatrix}});
    }
}
