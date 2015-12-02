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
package com.knime.bigdata.spark.node.pmml.transformation;

import java.util.List;

import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.PMMLTransformationJob;
import com.knime.bigdata.spark.node.pmml.PMMLAssignTask;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLTransformationTask extends PMMLAssignTask {

    private static final long serialVersionUID = 1L;
    private boolean m_replace;
    private Integer[] m_resultColIdxs2Add;
    private Integer[] m_inputColIdxs2Skip;

    /**
     * Transformation task.
     * @param addCols the indices of the result array to add to the result
     * @param replace <code>true</code> if processed columns should be replaced
     * @param skipCols the indices of the input columns to filter from the result
     */
    public PMMLTransformationTask(final List<Integer> addCols, final boolean replace,
        final List<Integer> skipCols) {
        super(PMMLTransformationJob.class.getCanonicalName());
        m_resultColIdxs2Add = addCols.toArray(new Integer[0]);
        m_replace = replace;
        m_inputColIdxs2Skip = skipCols.toArray(new Integer[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String[] getAdditionalInputParams() {
        return new String[]{PMMLTransformationJob.PARAM_REPLACE, Boolean.toString(m_replace),
            PMMLTransformationJob.PARAM_RESULT_COL_IDXS_2_ADD, JsonUtils.toJsonArray((Object[])m_resultColIdxs2Add),
            PMMLTransformationJob.PARAM_INPUT_COLS_2_SKIP, JsonUtils.toJsonArray((Object[])m_inputColIdxs2Skip)};
    }
}
