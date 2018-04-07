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
 *   Created on Jan 30, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.associationrule;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;

/**
 * Association rules learner settings container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SuppressWarnings("javadoc")
public class AssociationRuleLearnerSettings {

    /**
     * Minimal confidence for generating Association Rule. minConfidence will not affect the mining
     * for frequent itemsets, but will affect the association rules generation.
     */
    private static final String CFG_MIN_CONFIDENCE = "minConfidence";
    private static final double DEFAULT_MIN_CONFIDENCE = 0.8;
    private final SettingsModelDoubleBounded m_minConfidenceModel = new SettingsModelDoubleBounded(CFG_MIN_CONFIDENCE, DEFAULT_MIN_CONFIDENCE, 0.0, 1.0);
    public double getMinConfidence() { return m_minConfidenceModel.getDoubleValue(); }
    public SettingsModelDoubleBounded getMinConfidenceModel() { return m_minConfidenceModel; }


    /** Default constructor. */
    public AssociationRuleLearnerSettings() {
    }

    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_minConfidenceModel.saveSettingsTo(settings);
    }

    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_minConfidenceModel.validateSettings(settings);
    }

    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_minConfidenceModel.loadSettingsFrom(settings);
    }
}
