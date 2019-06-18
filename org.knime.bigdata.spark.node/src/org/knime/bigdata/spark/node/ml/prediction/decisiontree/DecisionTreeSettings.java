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

import java.util.Random;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.QualityMeasure;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings class for the Spark Decision Tree learner nodes (mllib, ml-classification and ml-regression). Which
 * settings are saved/loaded/validated depends on the provided {@link DecisionTreeLearnerMode}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DecisionTreeSettings extends MLlibNodeSettings {

    private final SettingsModelInteger m_maxTreeDepth =
        new SettingsModelIntegerBounded("maxDepth", 5, 1, Integer.MAX_VALUE);

    private final SettingsModelInteger m_maxNoOfBins =
        new SettingsModelIntegerBounded("maxNumBins", 32, 1, Integer.MAX_VALUE);

    private final SettingsModelString m_qualityMeasure =
        new SettingsModelString("qualityMeasure", EnumContainer.QualityMeasure.gini.name());

    private final SettingsModelDoubleBounded m_minInformationGain =
            new SettingsModelDoubleBounded("minInformationGain", 0, 0, Double.MAX_VALUE);

    private final SettingsModelInteger m_minRowsPerTreeNode =
        new SettingsModelIntegerBounded("minRowsPerTreeNode", 1, 1, Integer.MAX_VALUE);

    private final SettingsModelBoolean m_useStaticSeed = new SettingsModelBoolean("useStaticSeed", true);

    private final SettingsModelInteger m_seed = new SettingsModelInteger("seed", new Random().nextInt());

    @Deprecated
    private final SettingsModelBoolean m_isClassification = new SettingsModelBoolean("isClassification", true);

    private final DecisionTreeLearnerMode m_mode;

    /**
     * Constructor.
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
                    updateEnabledness();
                }
            });
        } else {
            m_useStaticSeed.addChangeListener(new ChangeListener() {
                @Override
                public void stateChanged(final ChangeEvent e) {
                    updateEnabledness();
                }
            });
        }
    }

    /**
     * Updates the enabledness of settings that are enabled based on the value of other settings.
     */
    protected void updateEnabledness() {
        if (m_mode == DecisionTreeLearnerMode.DEPRECATED) {
            getQualityMeasureModel().setEnabled(m_isClassification.getBooleanValue());
        } else {
            getSeedModel().setEnabled(m_useStaticSeed.getBooleanValue());
        }
    }

    /**
     * @return the maxDepth
     */
    public int getMaxDepth() {
        return m_maxTreeDepth.getIntValue();
    }

    /**
     * @return the maxNoOfBins
     */
    public int getMaxNoOfBins() {
        return m_maxNoOfBins.getIntValue();
    }

    /**
     * @return the minimum number of rows required for each child (of a decision tree node)
     */
    public int getMinRowsPerNodeChild() {
        return m_minRowsPerTreeNode.getIntValue();
    }

    /**
     * @return whether to learn a classification or regression tree.
     */
    @Deprecated
    public boolean isClassification() {
        return m_isClassification.getBooleanValue();
    }

    /**
     * @return the {@link QualityMeasure}
     */
    public QualityMeasure getQualityMeasure() {
        //        return m_qualityMeasure.getStringValue();
        return QualityMeasure.valueOf(m_qualityMeasure.getStringValue());
    }

    /**
     * @return the minimum information gain
     */
    public double getMinInformationGain() {
        return m_minInformationGain.getDoubleValue();
    }

    /**
     * @return the maxTreeDepthModel
     */
    public SettingsModelInteger getMaxTreeDepthModel() {
        return m_maxTreeDepth;
    }

    /**
     * @return the maxNoOfBinsModel
     */
    public SettingsModelInteger getMaxNoOfBinsModel() {
        return m_maxNoOfBins;
    }

    /**
     * @return the minimum number of rows required for each child (of a decision tree node)
     */
    public SettingsModelInteger getMinRowsPerNodeChildModel() {
        return m_minRowsPerTreeNode;
    }

    /**
     * @return the qualityMeasureModel
     */
    public SettingsModelString getQualityMeasureModel() {
        return m_qualityMeasure;
    }

    /**
     * @return the minimum information gain model
     */
    public SettingsModelDoubleBounded getMinInformationGainModel() {
        return m_minInformationGain;
    }

    /**
     *
     * @return whether to use a static seed or not.
     */
    public boolean useStaticSeed() {
        return m_useStaticSeed.getBooleanValue();
    }

    /**
     * @return settings model for whether to use a static seed or not.
     */
    public SettingsModelBoolean getUseStaticSeedModel() {
        return m_useStaticSeed;
    }

    /**
     * Changes the seed value.
     */
    public void nextSeed() {
        m_seed.setIntValue(new Random().nextInt());
    }

    /**
     * @return the seed
     */
    public int getSeed() {
        return m_seed.getIntValue();
    }

    /**
     * @return the seedModel
     */
    public SettingsModelInteger getSeedModel() {
        return m_seed;
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

        m_maxTreeDepth.saveSettingsTo(settings);
        m_maxNoOfBins.saveSettingsTo(settings);

        switch (m_mode) {
            case DEPRECATED:
                m_isClassification.saveSettingsTo(settings);
                m_qualityMeasure.saveSettingsTo(settings);
                break;
            case CLASSIFICATION:
                m_qualityMeasure.saveSettingsTo(settings);
                m_minInformationGain.saveSettingsTo(settings);
                m_minRowsPerTreeNode.saveSettingsTo(settings);
                m_useStaticSeed.saveSettingsTo(settings);
                m_seed.saveSettingsTo(settings);
                break;
            case REGRESSION:
                m_minInformationGain.saveSettingsTo(settings);
                m_minRowsPerTreeNode.saveSettingsTo(settings);
                m_useStaticSeed.saveSettingsTo(settings);
                m_seed.saveSettingsTo(settings);
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

        m_maxTreeDepth.validateSettings(settings);
        m_maxNoOfBins.validateSettings(settings);


        switch (m_mode) {
            case DEPRECATED:
                m_isClassification.validateSettings(settings);
                m_qualityMeasure.validateSettings(settings);
                break;
            case CLASSIFICATION:
                m_qualityMeasure.validateSettings(settings);
                m_minInformationGain.validateSettings(settings);
                m_minRowsPerTreeNode.validateSettings(settings);
                m_useStaticSeed.validateSettings(settings);
                m_seed.validateSettings(settings);
                break;
            case REGRESSION:
                m_minInformationGain.validateSettings(settings);
                m_minRowsPerTreeNode.validateSettings(settings);
                m_useStaticSeed.validateSettings(settings);
                m_seed.validateSettings(settings);
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

        m_maxTreeDepth.loadSettingsFrom(settings);
        m_maxNoOfBins.loadSettingsFrom(settings);

        switch (m_mode) {
            case DEPRECATED:
                m_isClassification.loadSettingsFrom(settings);
                m_qualityMeasure.loadSettingsFrom(settings);
                break;
            case CLASSIFICATION:
                m_qualityMeasure.loadSettingsFrom(settings);
                m_minInformationGain.loadSettingsFrom(settings);
                m_minRowsPerTreeNode.loadSettingsFrom(settings);
                m_useStaticSeed.loadSettingsFrom(settings);
                m_seed.loadSettingsFrom(settings);
                break;
            case REGRESSION:
                m_minInformationGain.loadSettingsFrom(settings);
                m_minRowsPerTreeNode.loadSettingsFrom(settings);
                m_useStaticSeed.loadSettingsFrom(settings);
                m_seed.loadSettingsFrom(settings);
                break;
        }
        updateEnabledness();
    }

    /**
     *
     * @return mode of this settings object.
     */
    protected DecisionTreeLearnerMode getMode() {
        return m_mode;
    }
}