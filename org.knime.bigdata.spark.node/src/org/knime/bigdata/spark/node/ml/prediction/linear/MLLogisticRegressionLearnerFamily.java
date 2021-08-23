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
 * Label distribution family.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public enum MLLogisticRegressionLearnerFamily implements ButtonGroupEnumInterface {

    /**
     * Automatically select the family based on the number of classes (num classes = 1 or 2 => binomial and
     * multinomial otherwise
     */
    AUTO("Auto", "Automatically select the family based on the number of classes", "auto"),

    /**
     * Binary logistic regression with pivoting.
     */
    BINOMINAL("Binomial", "Binary logistic regression with pivoting", "binomial"),

    /**
     * Multinomial logistic (softmax) regression without pivoting.
     */
    MULTINOMIAL("Multinomial", "Multinomial logistic (softmax) regression without pivoting", "multinomial");

    private final String m_text;

    private final String m_toolTip;

    private final String m_sparkName;

    MLLogisticRegressionLearnerFamily(final String text, final String toolTip, final String sparkName) {
        m_text = text;
        m_toolTip = toolTip;
        m_sparkName = sparkName;
    }

    /**
     * @return name of this function in spark
     */
    public String toSparkName() {
        return m_sparkName;
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
        return this == AUTO;
    }
}
