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
 *   Created on 23.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest;

import java.util.Random;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.knime.bigdata.spark.core.job.util.EnumContainer.FeatureSubsetStrategy;
import com.knime.bigdata.spark.core.job.util.EnumContainer.InformationGain;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.DecisionTreeSettings;

/**
 * @author Tobias Koetter, KNIME.com
 * @author Ole Ostergaard, KNIME.com
 */
public class RandomForestSettings extends DecisionTreeSettings {

    private final Random RND = new Random();

    private final SettingsModelInteger m_noOfTreesModel =
            new SettingsModelIntegerBounded("noOfTrees", 5, 1, Integer.MAX_VALUE);

    private final SettingsModelString m_featureSubsetStrategyModel = new SettingsModelString("featureSubsetStrategy",
        FeatureSubsetStrategy.auto.name());

    private final SettingsModelInteger m_seedModel = new SettingsModelInteger("seed", RND.nextInt());

    private final SettingsModel[] m_models =  new SettingsModel[] {m_noOfTreesModel, m_featureSubsetStrategyModel,
        m_seedModel};



    /**
     * {@inheritDoc}
     */
    @Override
    public InformationGain getQualityMeasure() {
        if (!isClassification()) {
            return InformationGain.variance;
        }
        return super.getQualityMeasure();
    }

    /**
     * @return the rND
     */
    public Random getRND() {
        return RND;
    }

    /**
     * @return the noOfTrees
     */
    public int getNoOfTrees() {
        return m_noOfTreesModel.getIntValue();
    }

    /**
     * @return the featureSubsetStragegy
     */
    public FeatureSubsetStrategy getFeatureSubsetStragegy() {
        final String stategy = m_featureSubsetStrategyModel.getStringValue();
        return FeatureSubsetStrategy.valueOf(stategy);
    }

    /**
     * Changes the seed value.
     */
    public void nextSeed() {
        m_seedModel.setIntValue(RND.nextInt());
    }

    /**
     * @return the seed
     */
    public int getSeed() {
        return m_seedModel.getIntValue();
    }

    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @throws InvalidSettingsException  if the settings are invalid
     */
    @Override
    public void check(final DataTableSpec tableSpec) throws InvalidSettingsException {
        super.check(tableSpec);
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
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException  if the settings are invalid
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        for (SettingsModel m : m_models) {
            m.validateSettings(settings);
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
    }

    /**
     * @return the noOfTreesModel
     */
    public SettingsModelInteger getNoOfTreesModel() {
        return m_noOfTreesModel;
    }

    /**
     * @return the featureSubsetStragegyModel
     */
    public SettingsModelString getFeatureSubsetStragegyModel() {
        return m_featureSubsetStrategyModel;
    }

    /**
     * @return the seedModel
     */
    public SettingsModelInteger getSeedModel() {
        return m_seedModel;
    }

    /**
     * @return the featureSubsetStrategyModel
     */
    public SettingsModelString getFeatureSubsetStrategyModel() {
        return m_featureSubsetStrategyModel;
    }
}
