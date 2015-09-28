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
 *   Created on 27.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.util.Arrays;
import java.util.Collection;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.knime.bigdata.spark.jobserver.jobs.AbstractTreeLearnerJob;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class DecisionTreeSettings {

    private final SettingsModelInteger m_maxDepthModel =
            new SettingsModelIntegerBounded("maxDepth", 5, 1, Integer.MAX_VALUE);
    private final SettingsModelInteger m_maxNoOfBinsModel =
            new SettingsModelIntegerBounded("maxNumBins", 32, 1, Integer.MAX_VALUE);
    private final SettingsModelString m_qualityMeasure =
            new SettingsModelString("qualityMeasure", AbstractTreeLearnerJob.VALUE_GINI);
    private final SettingsModelBoolean m_isClassificationModel = new SettingsModelBoolean("isClassification", true);
    private final SettingsModel[] m_models =
            new SettingsModel[] {m_maxDepthModel, m_maxNoOfBinsModel, m_qualityMeasure, m_isClassificationModel};

    private final DialogComponentNumber m_maxDepthComponent =
            new DialogComponentNumber(getMaxDepthModel(), "Max depth: ", 5, 5);
    private final DialogComponentNumber m_maxNoOfBinsComponent = new DialogComponentNumber(getMaxNoOfBinsModel(),
        "Max number of bins: ", 5, 5);
    private final DialogComponentStringSelection m_qualityMeasureComponent = new DialogComponentStringSelection(
        getQualityMeasureModel(), "Quality measure: ",
        new String[] {AbstractTreeLearnerJob.VALUE_GINI, AbstractTreeLearnerJob.VALUE_ENTROPY});
    private final DialogComponent m_isClassificationComponent = new DialogComponentBoolean(getIsClassificationModel(),
            "Is classification");
    private final DialogComponent[] m_components =  new DialogComponent[] {m_maxDepthComponent,
        m_maxNoOfBinsComponent, m_qualityMeasureComponent, m_isClassificationComponent};

    /**
     *
     */
    public DecisionTreeSettings() {
        m_isClassificationModel.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                getQualityMeasureModel().setEnabled(m_isClassificationModel.getBooleanValue());
            }
        });
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
     * @return the qualityMeasure
     */
    public String getQualityMeasure() {
        return m_qualityMeasure.getStringValue();
    }

    /**
     * @param settings the {@link NodeSettingsWO} to write to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        for (SettingsModel m : m_models) {
            m.saveSettingsTo(settings);
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException  if the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        for (SettingsModel m : m_models) {
            m.validateSettings(settings);
        }
    }

    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @throws InvalidSettingsException  if the settings are invalid
     */
    public void check(final DataTableSpec tableSpec) throws InvalidSettingsException {
        //nothing to check
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @param tableSpecs input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec tableSpecs)
            throws NotConfigurableException {
        for (DialogComponent c : m_components) {
            c.loadSettingsFrom(settings, new DataTableSpec[] {tableSpecs});
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        for (SettingsModel m : m_models) {
            m.loadSettingsFrom(settings);
        }
    }

    /**
     * @return the models
     */
    public Collection<SettingsModel> getModels() {
        return Arrays.asList(m_models);
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
     * @return the qualityMeasureModel
     */
    public SettingsModelString getQualityMeasureModel() {
        return m_qualityMeasure;
    }

    /**
     * @return the components
     */
    public Collection<DialogComponent> getComponents() {
        return Arrays.asList(m_components);
    }

    /**
     * @return the maxDepthComponent
     */
    public DialogComponentNumber getMaxDepthComponent() {
        return m_maxDepthComponent;
    }

    /**
     * @return the maxNoOfBinsComponent
     */
    public DialogComponentNumber getMaxNoOfBinsComponent() {
        return m_maxNoOfBinsComponent;
    }

    /**
     * @return the qualityMeasureComponent
     */
    public DialogComponentStringSelection getQualityMeasureComponent() {
        return m_qualityMeasureComponent;
    }

    /**
     * @return the isClassification
     */
    public boolean isClassification() {
        return m_isClassificationModel.getBooleanValue();
    }

    /**
     * @return the isClassificationModel
     */
    public SettingsModelBoolean getIsClassificationModel() {
        return m_isClassificationModel;
    }

    /**
     * @return the isClassificationComponent
     */
    public DialogComponent getIsClassificationComponent() {
        return m_isClassificationComponent;
    }
}