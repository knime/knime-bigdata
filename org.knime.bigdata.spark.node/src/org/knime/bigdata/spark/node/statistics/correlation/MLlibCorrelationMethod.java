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
 *   Created on 25.09.2015 by koetter
 */
package org.knime.bigdata.spark.node.statistics.correlation;

import org.knime.bigdata.spark.core.job.util.EnumContainer.CorrelationMethod;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public enum MLlibCorrelationMethod implements ButtonGroupEnumInterface {
    /** Pearson */
    PEARSON("Pearson", "", true, CorrelationMethod.Pearson),
    /** Spearman */
    SPEARMAN("Spearman", "", false, CorrelationMethod.Spearman);
    private String m_text;
    private String m_toolTip;
    private boolean m_isDefault;
    private CorrelationMethod m_method;

    MLlibCorrelationMethod(final String text, final String toolTip, final boolean isDefault,
        final CorrelationMethod method) {
        m_text = text;
        m_toolTip = toolTip;
        m_isDefault = isDefault;
        m_method = method;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getText() {
        return m_text;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getActionCommand() {
        return name();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getToolTip() {
        return m_toolTip;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDefault() {
        return m_isDefault;
    }

    /**
     * @return the method
     */
    public CorrelationMethod getMethod() {
        return m_method;
    }

    /**
     * @return the default method
     */
    public static MLlibCorrelationMethod getDefault() {
        MLlibCorrelationMethod[] values = values();
        for (MLlibCorrelationMethod method : values) {
            if (method.isDefault()) {
                return method;
            }
        }
        return PEARSON;
    }

    /**
     * @param actionCommand the action command
     * @return the method for the action command
     */
    public static MLlibCorrelationMethod get(final String actionCommand) {
        return MLlibCorrelationMethod.valueOf(actionCommand);
    }
}
