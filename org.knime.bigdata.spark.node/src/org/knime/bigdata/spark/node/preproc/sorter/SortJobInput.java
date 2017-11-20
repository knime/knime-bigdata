/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package com.knime.bigdata.spark.node.preproc.sorter;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author dwk
 */
@SparkClass
public class SortJobInput extends JobInput {

    /**
     * Paramless constructor for automatic deserialization.
     */
    public SortJobInput() {}

    private static final String FEATURE_COL_IDXS = "featureColIdxs";
    private static final String SORT_DIRECTION_IS_ASCENDING = "sortDirectionIsAscending";
    private static final String MISSING_TO_END = "missingToEnd";


    SortJobInput(final String namedInputObject, final String namedOutputObject, final Integer[] featureColIdxs,
        final Boolean[] isSortDirectionAscending, final Boolean missingToEnd) {
        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(FEATURE_COL_IDXS , featureColIdxs);
        set(SORT_DIRECTION_IS_ASCENDING, isSortDirectionAscending);
        set(MISSING_TO_END, missingToEnd);
    }

    /**
     * @return Get indices of the feature columns
     */
    public Integer[] getFeatureColIdxs() {
        return get(FEATURE_COL_IDXS);
    }

    /**
     * @return whether or not the sort direction is ascending
     */
    public Boolean[] isSortDirectionAscending() {
        return get(SORT_DIRECTION_IS_ASCENDING);
    }

    /**
     * @return return whether or not missing to end
     */
    public Boolean missingToEnd() {
        return get(MISSING_TO_END);
    }

//    SortTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final Boolean[] aSortDirectionIsAscending,
//        final boolean missingToEnd, final String aOutputTable) {
//        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, aSortDirectionIsAscending, missingToEnd,
//            aOutputTable);
//    }
//
//    SortTask(final SparkContextConfig aContext, final String aInputRDD, final Integer[] featureColIdxs,
//        final Boolean[] aSortDirectionIsAscending, final boolean missingToEnd, final String aOutputTable) {
//        m_context = aContext;
//        m_inputTableName = aInputRDD;
//        m_colIdx = featureColIdxs;
//        m_sortDirectionIsAscending = aSortDirectionIsAscending;
//        m_missingToEnd = missingToEnd;
//        m_outputTableName = aOutputTable;
//    }
//
//    void execute(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
//        final String jasonParams = paramsAsJason();
//        if (exec != null) {
//            exec.checkCanceled();
//        }
//        JobserverJobController.startJobAndWaitForResult(m_context, SortJob.class.getCanonicalName(), jasonParams, exec);
//    }
//
//    String paramsAsJason() {
//        return paramsAsJason(m_inputTableName, m_colIdx, m_sortDirectionIsAscending, m_missingToEnd, m_outputTableName);
//    }
//
//    /**
//     * (non-private for unit testing)
//     *
//     * @return Json representation of parameters
//     */
//    static String paramsAsJason(final String aInputTableName, final Integer[] aColIdxs,
//        final Boolean[] aSortDirectionIsAscending, final boolean missingToEnd, final String aOutputTable) {
//
//        final Object[] inputParamas =
//            new Object[]{ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])aColIdxs),
//                SortJob.PARAM_SORT_IS_ASCENDING, JsonUtils.toJsonArray((Object[])aSortDirectionIsAscending),
//                SortJob.PARAM_MISSING_TO_END, Boolean.valueOf(missingToEnd),
//                KnimeSparkJob.PARAM_INPUT_TABLE, aInputTableName};
//
//        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
//            ParameterConstants.PARAM_OUTPUT, new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTable}});
//    }
}
