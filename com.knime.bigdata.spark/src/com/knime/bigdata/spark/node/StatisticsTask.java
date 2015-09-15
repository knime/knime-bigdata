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
package com.knime.bigdata.spark.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.StatisticsJob;
import com.knime.bigdata.spark.jobserver.server.BoundedMultiVariateStatisticalSummary;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author dwk
 */
public class StatisticsTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final KNIMESparkContext m_context;

    private final Integer[] m_colIds;

    private final String m_inputTableName;

    /**
     *
     * @param inputRDD
     * @param aColIdxs
     */
    StatisticsTask(final SparkRDD inputRDD, final Integer[] aColIdxs) {
        this(inputRDD.getContext(), inputRDD.getID(), aColIdxs);
    }

    StatisticsTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] aColIds) {
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_colIds = aColIds;
    }

    BoundedMultiVariateStatisticalSummary execute(final ExecutionMonitor exec) throws GenericKnimeSparkException,
        CanceledExecutionException {
        final String jasonParams = paramsAsJason();
        if (exec != null) {
            exec.checkCanceled();
        }
        JobResult res =
            JobControler.startJobAndWaitForResult(m_context, StatisticsJob.class.getCanonicalName(), jasonParams, exec);
        return (BoundedMultiVariateStatisticalSummary)res.getObjectResult();
    }

    String paramsAsJason() {
        return paramsAsJason(m_inputTableName, m_colIds);
    }

    /**
     * (non-private for unit testing)
     *
     * @return Json representation of parameters
     */
    static String paramsAsJason(final String aInputTableName, final Integer[] aColIds) {

        if (aColIds == null) {
            throw new NullPointerException("Column indices required.");
        }

        final List<Object> inputParams = new ArrayList<>();
        inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
        inputParams.add(aInputTableName);
        inputParams.add(ParameterConstants.PARAM_COL_IDXS);
        inputParams.add(JsonUtils.toJsonArray((Object[])aColIds));
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT, new String[0]});
    }
}
