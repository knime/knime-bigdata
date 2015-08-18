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
package com.knime.bigdata.spark.node.mllib.prediction.bayes.naive;

import java.io.Serializable;

import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.NaiveBayesJob;
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
public class NaiveBayesTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer[] m_numericColIdx;

    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final String m_MatrixName;

    private final double m_lambda;

    private final int m_lableIx;

    NaiveBayesTask(final SparkRDD inputRDD, final int aLableIx, final Integer[] featureColIdxs, final double aLambda,
        final String aMatrix) {
        this(inputRDD.getContext(), inputRDD.getID(), aLableIx, featureColIdxs, aLambda, aMatrix);
    }

    NaiveBayesTask(final KNIMESparkContext aContext, final String aInputRDD, final int aLableIx,
        final Integer[] featureColIdxs, final double aLambda, final String aMatrix) {
        m_lambda = aLambda;
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_numericColIdx = featureColIdxs;
        m_MatrixName = aMatrix;
        m_lableIx = aLableIx;
    }

    NaiveBayesModel execute(final ExecutionMonitor exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String learnerParams = paramsAsJason();
        if (exec != null) {
            exec.checkCanceled();
        }
        final String jobId = JobControler.startJob(m_context, NaiveBayesJob.class.getCanonicalName(), learnerParams);
        final JobResult result = JobControler.waitForJobAndFetchResult(m_context, jobId, exec);

        return (NaiveBayesModel)result.getObjectResult();
    }

    String paramsAsJason() {
        return paramsAsJason(m_inputTableName, m_lableIx, m_numericColIdx, m_lambda, m_MatrixName);
    }

    /**
     * (non-private for unit testing)
     *
     * @param aInputTableName
     * @param aLabelColIndex
     * @param aNumericColIdx
     * @param aLambda
     * @param aMatrix
     * @return Json representation of parameters
     */
    static String paramsAsJason(final String aInputTableName, final int aLabelColIndex, final Integer[] aNumericColIdx,
        final double aLambda, final String aResultRdd) {

        final Object[] inputParamas =
            new Object[]{NaiveBayesJob.PARAM_LAMBDA, aLambda, ParameterConstants.PARAM_LABEL_INDEX, aLabelColIndex,
                ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])aNumericColIdx),
                KnimeSparkJob.PARAM_INPUT_TABLE, aInputTableName};

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aResultRdd}});
    }
}
