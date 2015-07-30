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
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.SGDJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
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

    private final String m_mappingTableName;

    private final String m_inputTableName;

    private final String[] m_colNames;

    private final int m_numIterations;

    private final double m_regularizationValue;

    private final String m_jobClassPath;

    SGDLearnerTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final List<String> aNumericColNames,
        final String classColName, final int classColIdx, final SparkRDD mappingRDD, final int aNumIterations,
        final double aRegularizationValue, final Class<? extends SGDJob> jobClass) {
        m_numIterations = aNumIterations;
        m_regularizationValue = aRegularizationValue;
        m_context = inputRDD.getContext();
        m_inputTableName = inputRDD.getID();
        m_numericColIdx = featureColIdxs;
        final List<String> allColNames = new LinkedList<>(aNumericColNames);
        allColNames.add(classColName);
        m_colNames = allColNames.toArray(new String[allColNames.size()]);
        m_classColIdx = classColIdx;
        m_mappingTableName = mappingRDD == null ? null : mappingRDD.getID();
        m_jobClassPath = jobClass.getCanonicalName();
    }

    Serializable execute(final ExecutionContext exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String learnerParams = learnerDef();
        final String jobId = JobControler.startJob(m_context, m_jobClassPath, learnerParams);
        final JobResult result = JobControler.waitForJobAndFetchResult(m_context, jobId, exec);
        return (Serializable)result.getObjectResult();
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     */

    private String learnerDef() {
        return learnerDef(m_inputTableName, m_mappingTableName, m_colNames, m_numericColIdx, m_classColIdx, m_numIterations, m_regularizationValue);
    }


    /**
     * (public for unit testing)
     * @param aInputTableName
     * @param aMappingTableName
     * @param aColNames
     * @param aNumericColIdx
     * @param aClassColIdx
     * @param aNumIterations
     * @param aRegularizationValue
     * @return Json representation of parameters
     */
    public static  String learnerDef(final String aInputTableName, final String aMappingTableName, final String[] aColNames, final Integer[] aNumericColIdx,
        final Integer aClassColIdx, final Integer aNumIterations, final Double aRegularizationValue) {

        final Object[] inputParamas;
        if (aMappingTableName == null) {
            inputParamas =
                new Object[]{ParameterConstants.PARAM_NUM_ITERATIONS, aNumIterations, ParameterConstants.PARAM_STRING,
                    aRegularizationValue, ParameterConstants.PARAM_LABEL_INDEX, aClassColIdx,
                    ParameterConstants.PARAM_COL_IDXS + ParameterConstants.PARAM_STRING,
                    JsonUtils.toJsonArray((Object[])aColNames), ParameterConstants.PARAM_COL_IDXS,
                    JsonUtils.toJsonArray((Object[])aNumericColIdx), ParameterConstants.PARAM_TABLE_1,
                    aInputTableName};
        } else {
            inputParamas =
                new Object[]{ParameterConstants.PARAM_NUM_ITERATIONS, aNumIterations, ParameterConstants.PARAM_STRING,
                    aRegularizationValue, ParameterConstants.PARAM_LABEL_INDEX, aClassColIdx,
                    ParameterConstants.PARAM_COL_IDXS + ParameterConstants.PARAM_STRING,
                    JsonUtils.toJsonArray((Object[])aColNames), ParameterConstants.PARAM_COL_IDXS,
                    JsonUtils.toJsonArray((Object[])aNumericColIdx), ParameterConstants.PARAM_TABLE_1,
                    aInputTableName, ParameterConstants.PARAM_TABLE_2, aMappingTableName};
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{}});
    }
}
