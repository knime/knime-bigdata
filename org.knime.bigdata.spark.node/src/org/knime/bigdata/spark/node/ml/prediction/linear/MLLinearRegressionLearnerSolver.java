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
 * ML-based linear learner solver algorithm for optimization.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public enum MLLinearRegressionLearnerSolver implements ButtonGroupEnumInterface {

    /**
     * Auto means that the solver algorithm is selected automatically.
     */
    AUTO("Auto", "auto"),

    /**
     * Limited-memory BFGS which is a limited-memory quasi-Newton optimization method.
     */
    LBFGS("Limited-memory BFGS", "l-bfgs"),

    /**
     * Normal Equation as an analytical solution to the linear regression problem.
     */
    NORMAL("Normal Equation", "normal");

    private final String m_text;

    private final String m_sparkName;

    MLLinearRegressionLearnerSolver(final String text, final String sparkName) {
        m_text = text;
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
        return null;
    }

    @Override
    public boolean isDefault() {
        return this == AUTO;
    }
}
