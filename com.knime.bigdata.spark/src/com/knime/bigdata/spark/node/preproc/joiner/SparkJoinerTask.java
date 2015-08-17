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

import javax.annotation.Nonnull;

import org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.JoinJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
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
        final Integer[] leftJoinColumns, final Integer[] rightJoinColumns, final Integer[] aSelectColIdxesLeft,
        final Integer[] aSelectColIdxesRight, final String aResultRDD) {
        this(aLeftRDD.getContext(), aLeftRDD.getID(), aRightRDD.getID(), aJoinMode, leftJoinColumns,
            rightJoinColumns, aSelectColIdxesLeft, aSelectColIdxesRight, aResultRDD);
        aLeftRDD.compatible(aRightRDD);
    }

    /**
     * (public for unit testing) stores references to given parameters
     *
     * @param aContext
     * @param aLeftRDD
     * @param aRightRDD
     * @param aJoinMode
     * @param leftJoinColumns
     * @param rightJoinColumns
     * @param aSelectColIdxesLeft
     * @param aSelectColIdxesRight
     * @param aResultRDD
     */
    public SparkJoinerTask(final KNIMESparkContext aContext, final String aLeftRDD, final String aRightRDD,
        final JoinMode aJoinMode, final Integer[] leftJoinColumns, final Integer[] rightJoinColumns,
        final Integer[] aSelectColIdxesLeft, final Integer[] aSelectColIdxesRight, final String aResultRDD) {

        m_context = aContext;
        m_LeftTableName = aLeftRDD;
        m_RightTableName = aRightRDD;
        m_ResultTableName = aResultRDD;
        m_JoinMode = aJoinMode;

        m_JoinColIdxesLeft = leftJoinColumns;
        m_JoinColIdxesRight = rightJoinColumns;
        m_SelectColIdxesLeft = aSelectColIdxesLeft;
        m_SelectColIdxesRight = aSelectColIdxesRight;
    }

    void execute(final ExecutionContext exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String joinParams = joinParams();
        exec.checkCanceled();
        final String jobId = JobControler.startJob(m_context, JoinJob.class.getCanonicalName(), joinParams);

        JobControler.waitForJobAndFetchResult(m_context, jobId, exec);
    }

    private String joinParams() {
        return joinParams(m_LeftTableName, m_RightTableName, m_JoinMode, m_JoinColIdxesLeft, m_JoinColIdxesRight,
            m_SelectColIdxesLeft, m_SelectColIdxesRight, m_ResultTableName);
    }

    /**
     * (unit testing)
     *
     * @param aLeftTableName
     * @param aRightTableName
     * @param aJoinMode
     * @param aJoinColIdxesLeft
     * @param aJoinColIdxesRight
     * @param aSelectColIdxesLeft
     * @param aSelectColIdxesRight
     * @param aResultTableName
     * @return Json representation of parameters
     */
    public static String joinParams(@Nonnull final String aLeftTableName, @Nonnull final String aRightTableName,
        @Nonnull final JoinMode aJoinMode, @Nonnull final Integer[] aJoinColIdxesLeft,
        @Nonnull final Integer[] aJoinColIdxesRight, @Nonnull final Integer[] aSelectColIdxesLeft,
        @Nonnull final Integer[] aSelectColIdxesRight, @Nonnull final String aResultTableName) {
        final Object[] inputParamas =
            {ParameterConstants.PARAM_TABLE_1, aLeftTableName, ParameterConstants.PARAM_TABLE_2, aRightTableName,
                ParameterConstants.PARAM_STRING, aJoinMode.toString(),
                ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 0),
                JsonUtils.toJsonArray((Object[])aJoinColIdxesLeft),
                ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 1),
                JsonUtils.toJsonArray((Object[])aJoinColIdxesRight),
                ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2),
                JsonUtils.toJsonArray((Object[])aSelectColIdxesLeft),
                ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 3),
                JsonUtils.toJsonArray((Object[])aSelectColIdxesRight)};

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, aResultTableName}});
    }

    /**
     * (unit testing only) check that all required parameters are properly set and can be verified (this does not make
     * any calls to the server)
     *
     * @return validation result
     */
    public SparkJobValidation validate() {
        return new JoinJob().validate(new JobConfig(ConfigFactory.parseString(joinParams())));
    }

}
