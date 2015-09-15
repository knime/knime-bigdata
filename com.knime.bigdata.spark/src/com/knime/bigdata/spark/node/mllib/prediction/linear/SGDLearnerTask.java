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
package com.knime.bigdata.spark.node.mllib.prediction.linear;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.SGDJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author koetter
 */
public class SGDLearnerTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer[] m_numericColIdx;

    private final int m_classColIdx;

    private final KNIMESparkContext m_context;

    private final NominalFeatureInfo m_nomFeatureInfo;

    private final String m_inputTableName;

    //private final String[] m_colNames;

    private final int m_numIterations;

    private final double m_regularizationValue;

    private final String m_jobClassPath;

    SGDLearnerTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final List<String> aNumericColNames,
        final String classColName, final int classColIdx, final NominalFeatureInfo nominalFeatureInfo, final int aNumIterations,
        final double aRegularizationValue, final Class<? extends SGDJob> jobClass) {
        m_numIterations = aNumIterations;
        m_regularizationValue = aRegularizationValue;
        m_context = inputRDD.getContext();
        m_inputTableName = inputRDD.getID();
        m_numericColIdx = featureColIdxs;
        final List<String> allColNames = new LinkedList<>(aNumericColNames);
        allColNames.add(classColName);
        //m_colNames = allColNames.toArray(new String[allColNames.size()]);
        m_classColIdx = classColIdx;
        m_nomFeatureInfo = nominalFeatureInfo;
        m_jobClassPath = jobClass.getCanonicalName();
    }

    Serializable execute(final ExecutionMonitor exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String learnerParams = learnerDef();
        exec.checkCanceled();
        final JobResult result = JobControler.startJobAndWaitForResult(m_context, m_jobClassPath, learnerParams, exec);
        return (Serializable)result.getObjectResult();
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     * @throws GenericKnimeSparkException
     */

    private String learnerDef() throws GenericKnimeSparkException {
        return learnerDef(m_inputTableName, m_nomFeatureInfo, m_numericColIdx, m_classColIdx,
            m_numIterations, m_regularizationValue);
    }


    /**
     * (public for unit testing)
     * @param aInputTableName
     * @param nomFeatureInfo the nominal column indices as first element and the number of unique values of the corresponding
     * column as second argument
     * @param aNumericColIdx
     * @param aClassColIdx
     * @param aNumIterations
     * @param aRegularizationValue
     * @return Json representation of parameters
     * @throws GenericKnimeSparkException
     */
    public static String learnerDef(final String aInputTableName, final NominalFeatureInfo nomFeatureInfo,
        final Integer[] aNumericColIdx, final Integer aClassColIdx,
        final Integer aNumIterations, final Double aRegularizationValue) throws GenericKnimeSparkException {

        final Object[] inputParamas;
        if (nomFeatureInfo == null) {
            inputParamas =
                    new Object[]{ParameterConstants.PARAM_NUM_ITERATIONS, aNumIterations,
                    SGDJob.PARAM_REGULARIZATION, aRegularizationValue,
                    ParameterConstants.PARAM_LABEL_INDEX, aClassColIdx,
                    //ParameterConstants.PARAM_COL_NAMES, JsonUtils.toJsonArray((Object[])aColNames),
                    ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])aNumericColIdx),
                    KnimeSparkJob.PARAM_INPUT_TABLE, aInputTableName};
        } else {
            inputParamas =
                new Object[]{ParameterConstants.PARAM_NUM_ITERATIONS, aNumIterations,
                SGDJob.PARAM_REGULARIZATION, aRegularizationValue,
                ParameterConstants.PARAM_LABEL_INDEX, aClassColIdx,
                //ParameterConstants.PARAM_COL_NAMES, JsonUtils.toJsonArray((Object[])aColNames),
                ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])aNumericColIdx),
                KnimeSparkJob.PARAM_INPUT_TABLE, aInputTableName,
                SupervisedLearnerUtils.PARAM_NOMINAL_FEATURE_INFO, JobConfig.encodeToBase64(nomFeatureInfo)};
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{}});
    }
}
