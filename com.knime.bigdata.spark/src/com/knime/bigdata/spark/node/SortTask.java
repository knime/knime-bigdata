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

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.SortJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author dwk
 */
public class SortTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final KNIMESparkContext m_context;

    private final Integer[] m_colIdx;

    private final Boolean[] m_sortDirectionIsAscending;

    private final String m_inputTableName;

    private final String m_outputTableName;

    SortTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final Boolean[] aSortDirectionIsAscending,
        final String aOutputTable) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, aSortDirectionIsAscending, aOutputTable);
    }

    SortTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] featureColIdxs,
        final Boolean[] aSortDirectionIsAscending, final String aOutputTable) {
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_colIdx = featureColIdxs;
        m_sortDirectionIsAscending = aSortDirectionIsAscending;
        m_outputTableName = aOutputTable;
    }

    void execute(final ExecutionMonitor exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String jasonParams = paramsAsJason();
        if (exec != null) {
            exec.checkCanceled();
        }
        JobControler.startJobAndWaitForResult(m_context, SortJob.class.getCanonicalName(), jasonParams, exec);
    }

    String paramsAsJason() {
        return paramsAsJason(m_inputTableName, m_colIdx, m_sortDirectionIsAscending, m_outputTableName);
    }

    /**
     * (non-private for unit testing)
     *
     * @return Json representation of parameters
     */
    static String paramsAsJason(final String aInputTableName, final Integer[] aColIdxs,
        final Boolean[] aSortDirectionIsAscending, final String aOutputTable) {

        final Object[] inputParamas =
            new Object[]{ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])aColIdxs),
                SortJob.PARAM_SORT_IS_ASCENDING, JsonUtils.toJsonArray((Object[])aSortDirectionIsAscending),
                KnimeSparkJob.PARAM_INPUT_TABLE, aInputTableName};

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTable}});
    }
}
