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
 *   Created on Jul 5, 2021 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.ml.prediction.linear;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * Linear learner regularizer.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public enum LinearLearnerRegularizer implements ButtonGroupEnumInterface {

    /** None (a.k.a. ordinary least squares), reg param = 0 */
    NONE("None"),

    /** Ridge using a given reg param and elastic net param = 0 */
    RIDGE("Ridge Regression (L2)"),

    /** Lasso using a given reg param and elastic net param = 1 */
    LASSO("Lasso (L1)"),

    /** Elastic net using a given reg and net param */
    ELASTIC_NET("Elastic Net (L1+L2)");


    private final String m_text;

    LinearLearnerRegularizer(final String text) {
        m_text = text;
    }

    @Override
    public String getText() {
        return m_text;
    }

    @Override
    public String getActionCommand() {
        return name();
    }

    @Override
    public String getToolTip() {
        return null;
    }

    @Override
    public boolean isDefault() {
        return this == RIDGE;
    }
}
