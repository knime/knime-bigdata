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
package com.knime.bigdata.spark.node.preproc.joiner;

import java.io.Serializable;

import org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.JoinJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author koetter
 */
public class SparkJoinerTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final KNIMESparkContext m_context;

    private final String m_LeftTableName;

    private final String m_RightTableName;

    private final JoinMode m_JoinMode;

    private final Integer[] m_JoinColIdxesLeft;

    private final Integer[] m_JoinColIdxesRight;

    private final Integer[] m_SelectColIdxesLeft;

    private final Integer[] m_SelectColIdxesRight;

    private String m_ResultTableName;

    SparkJoinerTask(final SparkRDD aLeftRDD, final SparkRDD aRightRDD, final JoinMode aJoinMode,
        final int[] aJoinColIdxesLeft, final int[] aJoinColIdxesRight,
        final Integer[] aSelectColIdxesLeft, final Integer[] aSelectColIdxesRight, final String aResultRDD) {
        this(aLeftRDD.getContext(), aLeftRDD.getID(), aRightRDD.getID(), aJoinMode, aJoinColIdxesLeft, aJoinColIdxesRight, aSelectColIdxesLeft, aSelectColIdxesRight, aResultRDD);
        aLeftRDD.compatible(aRightRDD);
    }

    /**
     * (public for unit testing)
     * stores references to given parameters
     * @param aContext
     * @param aLeftRDD
     * @param aRightRDD
     * @param aJoinMode
     * @param aJoinColIdxesLeft
     * @param aJoinColIdxesRight
     * @param aSelectColIdxesLeft
     * @param aSelectColIdxesRight
     * @param aResultRDD
     */
    public SparkJoinerTask(final KNIMESparkContext aContext, final String aLeftRDD, final String aRightRDD, final JoinMode aJoinMode,
        final int[] aJoinColIdxesLeft, final int[] aJoinColIdxesRight,
        final Integer[] aSelectColIdxesLeft, final Integer[] aSelectColIdxesRight, final String aResultRDD) {

        m_context = aContext;
        m_LeftTableName = aLeftRDD;
        m_RightTableName = aRightRDD;
        m_ResultTableName = aResultRDD;
        m_JoinMode = aJoinMode;

        m_JoinColIdxesLeft = copyIntoIntegerArray(aJoinColIdxesLeft);
        m_JoinColIdxesRight = copyIntoIntegerArray(aJoinColIdxesRight);
        m_SelectColIdxesLeft = aSelectColIdxesLeft;
        m_SelectColIdxesRight = aSelectColIdxesRight;
    }

    /**
     * @param aJoinColIdxesLeft
     */
    private static Integer[] copyIntoIntegerArray(final int[] aJoinColIdxesLeft) {
        Integer[] copy = new Integer[aJoinColIdxesLeft.length];
        for (int ix = 0; ix < aJoinColIdxesLeft.length; ix++) {
            copy[ix] = aJoinColIdxesLeft[ix];
        }
        return copy;
    }

    void execute(final ExecutionContext exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String joinParams = joinParams();
        final String jobId = JobControler.startJob(m_context, JoinJob.class.getCanonicalName(), joinParams);

        JobControler.waitForJobAndFetchResult(m_context, jobId, exec);
    }


    private String joinParams() {

        final Object[] inputParamas =
            {ParameterConstants.PARAM_TABLE_1, m_LeftTableName, ParameterConstants.PARAM_TABLE_2, m_RightTableName,
                ParameterConstants.PARAM_STRING, m_JoinMode.toString(),
                ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 0),
                JsonUtils.toJsonArray((Object[])m_JoinColIdxesLeft),
                ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 1),
                JsonUtils.toJsonArray((Object[])m_JoinColIdxesRight),
                ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2),
                JsonUtils.toJsonArray((Object[])m_SelectColIdxesLeft),
                ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 3),
                JsonUtils.toJsonArray((Object[])m_SelectColIdxesRight)};

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, m_ResultTableName}});
    }

    /**
     * (unit testing only)
     * check that all required parameters are properly set and can be verified (this does not
     * make any calls to the server)
     * @return validation result
     */
    public SparkJobValidation validate() {
        return new JoinJob().validate(ConfigFactory.parseString(joinParams()));
    }

}
