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
package com.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.knime.bigdata.spark.jobserver.jobs.AbstractTreeLearnerJob;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.EnsembleLossesType;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.DecisionTreeSettings;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class GradientBoostedTreeSettings extends DecisionTreeSettings {

    private final SettingsModelInteger m_noOfIterations =
            new SettingsModelIntegerBounded("noOfIterations", 5, 1, Integer.MAX_VALUE);

    private final SettingsModelDouble m_learningRate = new SettingsModelDoubleBounded("learningRate", 0.1, 0, 1);

    private final SettingsModelString m_lossFunction =
            new SettingsModelString("lossFunction", EnsembleLossesType.LogLoss.name());

    private final SettingsModel[] m_models =  new SettingsModel[] {m_noOfIterations, m_lossFunction, m_learningRate};

    private final DialogComponent m_noOfIterationsComponent =
            new DialogComponentNumber(getNoOfIterationsModel(), "Number of iterations", 5, 5);

    private final DialogComponent m_lossFunctionComponent = new DialogComponentStringSelection(
        getLossFunctionModel(), "Loss function", EnsembleLossesType.AbsoluteError.name(),
        EnsembleLossesType.LogLoss.name(), EnsembleLossesType.SquaredError.name());

    private final DialogComponent m_learningRateComponent =
            new DialogComponentNumber(getLearningRateModel(), "Learning rate", 0.01);

    private final DialogComponent[] m_components =  new DialogComponent[] {m_noOfIterationsComponent,
        m_lossFunctionComponent, m_learningRateComponent};

    /**
     * {@inheritDoc}
     */
    @Override
    public String getQualityMeasure() {
        if (!isClassification()) {
            return AbstractTreeLearnerJob.VALUE_VARIANCE;
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
     * @return the {@link EnsembleLossesType}
     */
    public EnsembleLossesType getLossFunction() {
        final String stategy = m_lossFunction.getStringValue();
        return EnsembleLossesType.valueOf(stategy);
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
     * @param tableSpec input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec tableSpec)
            throws NotConfigurableException {
        super.loadSettingsFrom(settings, tableSpec);
        for (DialogComponent c : m_components) {
            c.loadSettingsFrom(settings, new DataTableSpec[] {tableSpec});
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

    /**
     * @return the noOfIterationsComponent
     */
    public DialogComponent getNoOfIterationsComponent() {
        return m_noOfIterationsComponent;
    }

    /**
     * @return the lossFunctionComponent
     */
    public DialogComponent getLossFunctionComponent() {
        return m_lossFunctionComponent;
    }

    /**
     * @return the learningRateComponent
     */
    public DialogComponent getLearningRateComponent() {
        return m_learningRateComponent;
    }

    /**
     * @return the models
     */
    @Override
    protected Collection<SettingsModel> getModels() {
        final List<SettingsModel> modelList = Arrays.asList(m_models);
        modelList.addAll(super.getModels());
        return modelList;
    }

    /**
     * @return the components
     */
    @Override
    protected Collection<DialogComponent> getComponents() {
        final List<DialogComponent> list = Arrays.asList(m_components);
        list.addAll(super.getComponents());
        return list;
    }
}
