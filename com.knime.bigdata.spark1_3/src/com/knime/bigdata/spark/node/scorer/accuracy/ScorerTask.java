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
 */
package com.knime.bigdata.spark.node.scorer.accuracy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.ScorerJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NumericScorerData;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ScorerData;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 * computes classification / regression scores
 *
 * @author dwk
 */
public class ScorerTask implements Serializable {

    private static final long serialVersionUID = 1L;


    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final int m_actualColumnIdx;

    private final Integer m_predictionColumnIdx;

    private final Boolean m_isClassification;

    /**
     * @param inputRDD
     * @param aActualColumnIdx
     * @param aPredictionColumnIdx
     * @param aIsClassification
     */
    public ScorerTask(final SparkRDD inputRDD, final Integer aActualColumnIdx, final Integer aPredictionColumnIdx,
        final Boolean aIsClassification) {
        this(inputRDD.getContext(), inputRDD.getID(), aActualColumnIdx, aPredictionColumnIdx, aIsClassification            );
    }

    //unit testing constructor only
    ScorerTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer aActualColumnIdx,
        final Integer aPredictionColumnIdx, final Boolean aIsClassification) {
        m_actualColumnIdx = aActualColumnIdx;
        m_predictionColumnIdx = aPredictionColumnIdx;
        m_isClassification = aIsClassification;
        m_context = aContext;
        m_inputTableName = aInputRDD;
    }

    /**
     * @param exec
     * @return
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public Serializable execute(final ExecutionMonitor exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String learnerParams = learnerDef();
        if (exec != null) {
            exec.checkCanceled();
        }
        JobResult res = JobControler.startJobAndWaitForResult(m_context, ScorerJob.class.getCanonicalName(), learnerParams, exec);
        if (m_isClassification) {
            return (ScorerData)res.getObjectResult();
        } else {
            return (NumericScorerData)res.getObjectResult();
        }
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     *
     * @throws GenericKnimeSparkException
     */
    String learnerDef() throws GenericKnimeSparkException {
        return paramsAsJason(m_inputTableName, m_actualColumnIdx, m_predictionColumnIdx, m_isClassification);
    }

    static String paramsAsJason(final String aInputRDD, final Integer aActualColumnIdx,
        final Integer aPredictionColumnIdx, final Boolean aIsClassification)
        throws GenericKnimeSparkException {
        final List<Object> inputParams = new ArrayList<>();
        if (aInputRDD != null) {
            inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
            inputParams.add(aInputRDD);
        }

        if (aActualColumnIdx != null) {
            inputParams.add(ScorerJob.PARAM_ACTUAL_COL_INDEX);
            inputParams.add(aActualColumnIdx);
        }

        if (aPredictionColumnIdx != null) {
            inputParams.add(ScorerJob.PARAM_PREDICTION_COL_INDEX);
            inputParams.add(aPredictionColumnIdx);
        }
        if (aIsClassification != null) {
            inputParams.add(ScorerJob.PARAM_IS_CLASSIFICATION);
            inputParams.add(aIsClassification);
        }

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT, new String[]{}});
    }

}
