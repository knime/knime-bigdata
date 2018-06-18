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

import org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrequentItemSetSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;

/**
 * Association rules learner settings container. Contains settings for frequent item sets and association rules
 * generation.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkAssociationRuleLearnerSettings extends SparkFrequentItemSetSettings {

    private static final String CFG_MIN_CONFIDENCE = "minConfidence";
    private static final double DEFAULT_MIN_CONFIDENCE = 0.8;

    private final SettingsModelDoubleBounded m_minConfidenceModel =
        new SettingsModelDoubleBounded(CFG_MIN_CONFIDENCE, DEFAULT_MIN_CONFIDENCE, 0.0, 1.0);

    /**
     * The generated association rules can be filtered by a required minimal confidence. Confidence is an indication of
     * how often an association rule has been found to be true. For example, if in the transactions item set X appears 4
     * times, X and Y co-occur only 2 times, the confidence for the rule X => Y is then 2/4 = 0.5.
     *
     * @return minimal confidence in [0.0, 1.0]
     */
    public double getMinConfidence() {
        return m_minConfidenceModel.getDoubleValue();
    }

    /** @return minimal confidence model */
    public SettingsModelDoubleBounded getMinConfidenceModel() {
        return m_minConfidenceModel;
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        super.saveAdditionalSettingsTo(settings);
        m_minConfidenceModel.saveSettingsTo(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateAdditionalSettings(settings);
        m_minConfidenceModel.validateSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadAdditionalValidatedSettingsFrom(settings);
        m_minConfidenceModel.loadSettingsFrom(settings);
    }
}
