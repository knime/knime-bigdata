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
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.DecisionTreeLearner;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author koetter
 */
public class DecisionTreeTask implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer[] m_numericColIdx;
    private final int m_classColIdx;
    private final String m_classColName;
    private final KNIMESparkContext m_context;
    private String m_outputTableName;
    private String m_mappingTableName;
    private String m_inputTableName;
    private final String[] m_colNames;

    DecisionTreeTask(final SparkRDD inputRDD, final List<Integer> numericColIdx, final List<String> aNumericColNames, final String classColName,
        final int classColIdx, final SparkRDD mappingRDD, final SparkRDD outputRDD) {
        if (!inputRDD.compatible(outputRDD)) {
            throw new IllegalArgumentException("Incompatible rdds");
        }
        m_context = inputRDD.getContext();
        m_inputTableName = inputRDD.getID();
        m_numericColIdx = numericColIdx.toArray(new Integer[numericColIdx.size()]);
        m_classColName = classColName;
        aNumericColNames.add(classColName);
        m_colNames = aNumericColNames.toArray(new String[aNumericColNames.size()]);
        m_classColIdx = classColIdx;
        m_outputTableName = outputRDD.getID();
        m_mappingTableName = mappingRDD.getID();
    }

    DecisionTreeModel execute(final ExecutionContext exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String learnerParams = learnerDef();
        final String jobId =
                JobControler.startJob(m_context, DecisionTreeLearner.class.getCanonicalName(), learnerParams);

        final JobResult result = JobControler.waitForJobAndFetchResult(m_context, jobId, exec);

        return (DecisionTreeModel)result.getObjectResult();
    }

        /**
         * names of the columns (must include label column), required for value mapping info
         */

        private String learnerDef() {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{ParameterConstants.PARAM_INFORMATION_GAIN, ParameterConstants.VALUE_GINI,
                ParameterConstants.PARAM_MAX_BINS, "6",
                ParameterConstants.PARAM_MAX_DEPTH, "8", ParameterConstants.PARAM_LABEL_INDEX,
                m_classColIdx,
                ParameterConstants.PARAM_COL_IDXS+ParameterConstants.PARAM_STRING, JsonUtils.toJsonArray(m_colNames),
                ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray(m_numericColIdx),
                ParameterConstants.PARAM_TABLE_1, m_inputTableName,
                ParameterConstants.PARAM_TABLE_2, m_mappingTableName},
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, m_outputTableName}});
    }

}
