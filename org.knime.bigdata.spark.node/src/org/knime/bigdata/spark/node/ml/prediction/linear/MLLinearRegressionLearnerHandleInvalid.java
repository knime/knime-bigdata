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
 * ML-based linear learner invalid data handling parameter.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public enum MLLinearRegressionLearnerHandleInvalid implements ButtonGroupEnumInterface {

    /**
     * Skip rows with missing values in any of the input columns.
     */
    SKIP("Ignore", "Filter out rows with missing values in any of the input columns."),

    /**
     * Keep rows with missing values and create an additional missing value label.
     */
    // unsupported: KEEP("Keep rows", "Handle missing values as additional label."),

    /**
     * Fail on rows with missing values in any of the input columns.
     */
    ERROR("Fail", "Fail on observing rows with missing values in any of the input columns.");

    private final String m_text;
    private final String m_toolTip;

    MLLinearRegressionLearnerHandleInvalid(final String text, final String tooltip) {
        m_text = text;
        m_toolTip = tooltip;
    }

    /**
     * @return name of this function in spark
     */
    public String toSparkName() {
        return name().toLowerCase();
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
        return m_toolTip;
    }

    @Override
    public boolean isDefault() {
        return this == ERROR;
    }
}
