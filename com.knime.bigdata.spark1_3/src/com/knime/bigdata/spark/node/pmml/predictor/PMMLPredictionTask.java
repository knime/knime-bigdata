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
package com.knime.bigdata.spark.node.pmml.predictor;

import com.knime.bigdata.spark.jobserver.jobs.PMMLPredictionJob;
import com.knime.bigdata.spark.node.pmml.PMMLAssignTask;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLPredictionTask extends PMMLAssignTask {

    private static final long serialVersionUID = 1L;
    private boolean m_appendProbabilities;

    /**
     * Constructor.
     * @param appendProbabilities <code>true</code> if probability columns should be added
     */
    public PMMLPredictionTask(final boolean appendProbabilities) {
        super(PMMLPredictionJob.class.getCanonicalName());
        m_appendProbabilities = appendProbabilities;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String[] getAdditionalInputParams() {
        return new String[]{PMMLPredictionJob.PARAM_APPEND_PROBABILITIES, Boolean.toString(m_appendProbabilities)};
    }
}
