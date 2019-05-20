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
 *   Created on 23.08.2015 by koetter
 */
package org.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.InformationGain;
import org.knime.bigdata.spark.core.job.util.EnumContainer.LossFunction;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class GradientBoostedTreesSettings extends DecisionTreeSettings {

    private final SettingsModelInteger m_noOfIterations =
            new SettingsModelIntegerBounded("noOfIterations", 5, 1, Integer.MAX_VALUE);

    private final SettingsModelDouble m_learningRate = new SettingsModelDoubleBounded("learningRate", 0.1, 0, 1);

    private final SettingsModelString m_lossFunction =
            new SettingsModelString("lossFunction", EnumContainer.LossFunction.LogLoss.name());

    private final SettingsModel[] m_models =  new SettingsModel[] {m_noOfIterations, m_lossFunction, m_learningRate};


    public GradientBoostedTreesSettings() {
        super(DecisionTreeLearnerMode.DEPRECATED);
    }

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
     * @return the noOfIterations
     */
    public int getNoOfIterations() {
        return m_noOfIterations.getIntValue();
    }

    /**
     * @return the {@link LossFunction}
     */
    public LossFunction getLossFunction() {
        final String stategy = m_lossFunction.getStringValue();
        return LossFunction.valueOf(stategy);
    }

    /**
     * @return the seed
     */
    public double getLearningRate() {
        return m_learningRate.getDoubleValue();
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
     * @param tableSpec the original input {@link DataTableSpec}
     * @throws InvalidSettingsException  if the settings are invalid
     */
    @Override
    public void check(final DataTableSpec tableSpec) throws InvalidSettingsException {
        super.check(tableSpec);
        // nothing to check
    }

    /**
     * @return the noOfIterations
     */
    public SettingsModelInteger getNoOfIterationsModel() {
        return m_noOfIterations;
    }

    /**
     * @return the featureSubsetStragegyModel
     */
    public SettingsModelString getLossFunctionModel() {
        return m_lossFunction;
    }

    /**
     * @return the seedModel
     */
    public SettingsModelDouble getLearningRateModel() {
        return m_learningRate;
    }
}
