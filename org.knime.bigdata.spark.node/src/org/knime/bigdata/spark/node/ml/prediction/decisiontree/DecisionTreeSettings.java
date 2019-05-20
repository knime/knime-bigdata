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
 *   Created on 27.09.2015 by koetter
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.InformationGain;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings class for both the (deprecated) MLlib as well as the ML decision tree learner nodes.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DecisionTreeSettings extends MLlibNodeSettings {

    private final SettingsModelInteger m_maxDepthModel =
        new SettingsModelIntegerBounded("maxDepth", 5, 1, Integer.MAX_VALUE);

    private final SettingsModelInteger m_maxNoOfBinsModel =
        new SettingsModelIntegerBounded("maxNumBins", 32, 1, Integer.MAX_VALUE);

    private final SettingsModelString m_qualityMeasure =
        new SettingsModelString("qualityMeasure", EnumContainer.InformationGain.gini.name());

    private final SettingsModelInteger m_minRowsPerNodeChild =
        new SettingsModelIntegerBounded("minRowsPerNodeChild", 1, 1, Integer.MAX_VALUE);

    @Deprecated
    private final SettingsModelBoolean m_isClassification = new SettingsModelBoolean("isClassification", true);

    private final SettingsModel[] m_models = new SettingsModel[]{m_maxDepthModel, m_maxNoOfBinsModel};

    private final DecisionTreeLearnerMode m_mode;

    /**
     * Constructor
     *
     * @param mode Whether this is a settings model for the (deprecated) MLlib learner, or the new ML learner nodes
     */
    public DecisionTreeSettings(final DecisionTreeLearnerMode mode) {
        super(true, mode != DecisionTreeLearnerMode.DEPRECATED);
        m_mode = mode;
        if (mode == DecisionTreeLearnerMode.DEPRECATED) {
            m_isClassification.addChangeListener(new ChangeListener() {
                @Override
                public void stateChanged(final ChangeEvent e) {
                    getQualityMeasureModel().setEnabled(m_isClassification.getBooleanValue());
                }
            });
        }
    }

    /**
     * @return the maxDepth
     */
    public int getMaxDepth() {
        return m_maxDepthModel.getIntValue();
    }

    /**
     * @return the maxNoOfBins
     */
    public int getMaxNoOfBins() {
        return m_maxNoOfBinsModel.getIntValue();
    }

    /**
     * @return the minimum number of rows required for each child (of a decision tree node)
     */
    public int getMinRowsPerNodeChild() {
        return m_maxNoOfBinsModel.getIntValue();
    }

    /**
     * @return whether to learn a classification or regression tree.
     */
    @Deprecated
    public boolean isClassification() {
        return m_isClassification.getBooleanValue();
    }

    /**
     * @return the {@link InformationGain}
     */
    public InformationGain getQualityMeasure() {
        //        return m_qualityMeasure.getStringValue();
        return InformationGain.valueOf(m_qualityMeasure.getStringValue());
    }

    /**
     * @return the maxDepthModel
     */
    public SettingsModelInteger getMaxDepthModel() {
        return m_maxDepthModel;
    }

    /**
     * @return the maxNoOfBinsModel
     */
    public SettingsModelInteger getMaxNoOfBinsModel() {
        return m_maxNoOfBinsModel;
    }

    /**
     * @return the minimum number of rows required for each child (of a decision tree node)
     */
    public SettingsModelInteger getMinRowsPerNodeChildModel() {
        return m_minRowsPerNodeChild;
    }

    /**
     * @return the qualityMeasureModel
     */
    public SettingsModelString getQualityMeasureModel() {
        return m_qualityMeasure;
    }

    /**
     * @return the isClassificationModel
     */
    @Deprecated
    public SettingsModelBoolean getIsClassificationModel() {
        return m_isClassification;
    }

    /**
     * @param settings the {@link NodeSettingsWO} to write to
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        for (SettingsModel m : m_models) {
            m.saveSettingsTo(settings);
        }

        switch (m_mode) {
            case DEPRECATED:
                m_isClassification.saveSettingsTo(settings);
                m_qualityMeasure.saveSettingsTo(settings);
                break;
            case CLASSIFICATION:
                m_qualityMeasure.saveSettingsTo(settings);
                m_minRowsPerNodeChild.saveSettingsTo(settings);
                break;
            case REGRESSION:
                m_minRowsPerNodeChild.saveSettingsTo(settings);
                break;
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        for (SettingsModel m : m_models) {
            m.validateSettings(settings);
        }

        switch (m_mode) {
            case DEPRECATED:
                m_isClassification.validateSettings(settings);
                m_qualityMeasure.validateSettings(settings);
                break;
            case CLASSIFICATION:
                m_qualityMeasure.validateSettings(settings);
                m_minRowsPerNodeChild.validateSettings(settings);
                break;
            case REGRESSION:
                m_minRowsPerNodeChild.validateSettings(settings);
                break;
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);
        for (SettingsModel m : m_models) {
            m.loadSettingsFrom(settings);
        }

        switch (m_mode) {
            case DEPRECATED:
                m_isClassification.loadSettingsFrom(settings);
                m_qualityMeasure.loadSettingsFrom(settings);
                break;
            case CLASSIFICATION:
                m_qualityMeasure.loadSettingsFrom(settings);
                m_minRowsPerNodeChild.loadSettingsFrom(settings);
                break;
            case REGRESSION:
                m_minRowsPerNodeChild.loadSettingsFrom(settings);
                break;
        }
    }
}