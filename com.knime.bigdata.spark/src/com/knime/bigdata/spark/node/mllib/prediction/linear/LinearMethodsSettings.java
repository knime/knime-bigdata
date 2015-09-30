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
 *   Created on 28.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.linear;

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
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;

import com.knime.bigdata.spark.node.mllib.MLlibNodeSettings;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LinearMethodsSettings extends MLlibNodeSettings {

    private final SettingsModelInteger m_noOfIterationsModel =
            new SettingsModelIntegerBounded("numberOfIteration", 100, 1, Integer.MAX_VALUE);

    private final SettingsModelDouble m_regularizationModel = new SettingsModelDouble("regularization", 0);

    private final SettingsModel[] m_models =
            new SettingsModel[] {m_noOfIterationsModel, m_regularizationModel};


    private final DialogComponentNumber m_noOfIterationsComponent = new DialogComponentNumber(m_noOfIterationsModel,
        "Number of iterations: ", 10);

    private final DialogComponentNumber m_regularizationComponent =
            new DialogComponentNumber(m_regularizationModel, "Regularizer: ", 0.001);

    private final DialogComponent[] m_components =  new DialogComponent[] {m_regularizationComponent,
        m_noOfIterationsComponent};

    /**
     * Constructor.
     */
    public LinearMethodsSettings() {
        super(true);
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
     * @param tableSpec the original input {@link DataTableSpec}
     * @throws InvalidSettingsException  if the settings are invalid
     */
    @Override
    public void check(final DataTableSpec tableSpec) throws InvalidSettingsException {
        super.check(tableSpec);
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @param tableSpecs input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec tableSpecs)
            throws NotConfigurableException {
        super.loadSettingsFrom(settings, tableSpecs);
        for (DialogComponent c : m_components) {
            c.loadSettingsFrom(settings, new DataTableSpec[] {tableSpecs});
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

    /**
     * @return the number of iterations
     */
    public int getNoOfIterations() {
        return m_noOfIterationsModel.getIntValue();
    }

    /**
     * @return the regularization model
     */
    public double getRegularization() {
        return m_regularizationModel.getDoubleValue();
    }

    /**
     * @return the noOfIterationsModel
     */
    public SettingsModelInteger getNoOfIterationsModel() {
        return m_noOfIterationsModel;
    }

    /**
     * @return the regularizationModel
     */
    public SettingsModelDouble getRegularizationModel() {
        return m_regularizationModel;
    }

    /**
     * @return the noOfIterationsComponent
     */
    public DialogComponentNumber getNoOfIterationsComponent() {
        return m_noOfIterationsComponent;
    }

    /**
     * @return the regularizationComponent
     */
    public DialogComponentNumber getRegularizationComponent() {
        return m_regularizationComponent;
    }

}
