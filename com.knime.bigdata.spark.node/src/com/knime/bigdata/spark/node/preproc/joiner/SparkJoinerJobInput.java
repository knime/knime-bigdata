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

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author koetter
 */
@SparkClass
public class SparkJoinerJobInput extends JobInput {

//    private static final String LEFT_TABLE_NAME = "leftTableName";
//    private static final String RIGHT_TABLE_NAME = "rightTableName";
    private static final String JOIN_MODE = "joinMode";
    private static final String JOIN_COL_IDXS_LEFT = "joinColIdxsLeft";
    private static final String JOIN_COL_IDXS_RIGHT = "joinColIdxsRight";
    private static final String SELECT_COL_IDXS_LEFT = "selectColIdxsLeft";
    private static final String SELECT_COL_IDXS_RIGHT = "selectColIdxsRight";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public SparkJoinerJobInput() {}

    SparkJoinerJobInput(final String aLeftRDD, final String aRightRDD, final JoinMode aJoinMode,
        final Integer[] leftJoinColumns, final Integer[] rightJoinColumns, final Integer[] aSelectColIdxesLeft,
        final Integer[] aSelectColIdxesRight, final String aResultRDD, final IntermediateSpec resultSpec) {
        addNamedInputObject(aLeftRDD);
        addNamedInputObject(aRightRDD);
        addNamedOutputObject(aResultRDD);
        withSpec(aResultRDD, resultSpec);
        set(JOIN_MODE, aJoinMode.name());
        set(JOIN_COL_IDXS_LEFT,leftJoinColumns);
        set(JOIN_COL_IDXS_RIGHT, rightJoinColumns);
        set(SELECT_COL_IDXS_LEFT, aSelectColIdxesLeft);
        set(SELECT_COL_IDXS_RIGHT, aSelectColIdxesRight);
    }

    /**
     * @return the name of the left input object
     */
    public String getLeftInputObject() {
        return getNamedInputObjects().get(0);
    }

    /**
     * @return the name of the right input object
     */
    public String getRightNamedObject() {
        return getNamedInputObjects().get(1);
    }

    /**
     * @return the join mode
     */
    public JoinMode getJoineMode() {
        final String methodName = get(JOIN_MODE);
        return JoinMode.valueOf(methodName);
    }


    /**
     * @return the join columns from the left table
     */
    public Integer[] getJoinColIdxsLeft() {
        return get(JOIN_COL_IDXS_LEFT);
    }

    /**
     * @return the join columns from the right table
     */
    public Integer[] getJoinColIdxsRight() {
        return get(JOIN_COL_IDXS_RIGHT);
    }

    /**
     * @return the selected column indices from the left table
     */
    public Integer[] getSelectColIdxsLeft() {
        return get(SELECT_COL_IDXS_LEFT);
    }

    /**
     * @return the selected column indices from the right table
     */
    public Integer[] getSelectColIdxsRight() {
        return get(SELECT_COL_IDXS_RIGHT);
    }
//
//    private final SparkContextConfig m_context;
//
//    private final String m_LeftTableName;
//
//    private final String m_RightTableName;
//
//    private final JoinMode m_JoinMode;
//
//    private final Integer[] m_JoinColIdxesLeft;
//
//    private final Integer[] m_JoinColIdxesRight;
//
//    private final Integer[] m_SelectColIdxesLeft;
//
//    private final Integer[] m_SelectColIdxesRight;
//
//    private String m_ResultTableName;
//
//    SparkJoinerJobInput(final SparkRDD aLeftRDD, final SparkRDD aRightRDD, final JoinMode aJoinMode,
//        final Integer[] leftJoinColumns, final Integer[] rightJoinColumns, final Integer[] aSelectColIdxesLeft,
//        final Integer[] aSelectColIdxesRight, final String aResultRDD) {
//        this(aLeftRDD.getContext(), aLeftRDD.getID(), aRightRDD.getID(), aJoinMode, leftJoinColumns,
//            rightJoinColumns, aSelectColIdxesLeft, aSelectColIdxesRight, aResultRDD);
//        aLeftRDD.compatible(aRightRDD);
//    }
//
//    /**
//     * (public for unit testing) stores references to given parameters
//     *
//     * @param aContext
//     * @param aLeftRDD
//     * @param aRightRDD
//     * @param aJoinMode
//     * @param leftJoinColumns
//     * @param rightJoinColumns
//     * @param aSelectColIdxesLeft
//     * @param aSelectColIdxesRight
//     * @param aResultRDD
//     */
//    public SparkJoinerJobInput(final SparkContextConfig aContext, final String aLeftRDD, final String aRightRDD,
//        final JoinMode aJoinMode, final Integer[] leftJoinColumns, final Integer[] rightJoinColumns,
//        final Integer[] aSelectColIdxesLeft, final Integer[] aSelectColIdxesRight, final String aResultRDD) {
//
//        m_context = aContext;
//        m_LeftTableName = aLeftRDD;
//        m_RightTableName = aRightRDD;
//        m_ResultTableName = aResultRDD;
//        m_JoinMode = aJoinMode;
//
//        m_JoinColIdxesLeft = leftJoinColumns;
//        m_JoinColIdxesRight = rightJoinColumns;
//        m_SelectColIdxesLeft = aSelectColIdxesLeft;
//        m_SelectColIdxesRight = aSelectColIdxesRight;
//    }
//
//    void execute(final ExecutionContext exec) throws KNIMESparkException, CanceledExecutionException {
//        final String joinParams = joinParams();
//        exec.checkCanceled();
//        JobserverJobController.startJobAndWaitForResult(m_context, JoinJob.class.getCanonicalName(), joinParams, exec);
//    }
//
//    private String joinParams() {
//        return joinParams(m_LeftTableName, m_RightTableName, m_JoinMode, m_JoinColIdxesLeft, m_JoinColIdxesRight,
//            m_SelectColIdxesLeft, m_SelectColIdxesRight, m_ResultTableName);
//    }
//
//    /**
//     * (unit testing)
//     *
//     * @param aLeftTableName
//     * @param aRightTableName
//     * @param aJoinMode
//     * @param aJoinColIdxesLeft
//     * @param aJoinColIdxesRight
//     * @param aSelectColIdxesLeft
//     * @param aSelectColIdxesRight
//     * @param aResultTableName
//     * @return Json representation of parameters
//     */
//    public static String joinParams(@Nonnull final String aLeftTableName, @Nonnull final String aRightTableName,
//        @Nonnull final JoinMode aJoinMode, @Nonnull final Integer[] aJoinColIdxesLeft,
//        @Nonnull final Integer[] aJoinColIdxesRight, @Nonnull final Integer[] aSelectColIdxesLeft,
//        @Nonnull final Integer[] aSelectColIdxesRight, @Nonnull final String aResultTableName) {
//        final Object[] inputParamas =
//            {JoinJob.PARAM_LEFT_RDD, aLeftTableName, JoinJob.PARAM_RIGHT_RDD, aRightTableName,
//                JoinJob.PARAM_JOIN_MODE, aJoinMode.toString(),
//                JoinJob.PARAM_JOIN_INDEXES_LEFT,
//                JsonUtils.toJsonArray((Object[])aJoinColIdxesLeft),
//                JoinJob.PARAM_JOIN_INDEXES_RIGHT,
//                JsonUtils.toJsonArray((Object[])aJoinColIdxesRight),
//                JoinJob.PARAM_SELECT_INDEXES_LEFT,
//                JsonUtils.toJsonArray((Object[])aSelectColIdxesLeft),
//                JoinJob.PARAM_SELECT_INDEXES_RIGHT,
//                JsonUtils.toJsonArray((Object[])aSelectColIdxesRight)};
//
//        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
//            ParameterConstants.PARAM_OUTPUT, new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aResultTableName}});
//    }
//
//    /**
//     * (unit testing only) check that all required parameters are properly set and can be verified (this does not make
//     * any calls to the server)
//     *
//     * @return validation result
//     */
//    public SparkJobValidation validate() {
//        return new JoinJob().validate(new JobConfig(ConfigFactory.parseString(joinParams())));
//    }

}
