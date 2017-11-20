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
package org.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.InformationGain;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @author Ole Ostergaard, KNIME.com
 */
public class DecisionTreeSettings extends MLlibNodeSettings {

    private final SettingsModelInteger m_maxDepthModel =
            new SettingsModelIntegerBounded("maxDepth", 5, 1, Integer.MAX_VALUE);
    private final SettingsModelInteger m_maxNoOfBinsModel =
            new SettingsModelIntegerBounded("maxNumBins", 32, 1, Integer.MAX_VALUE);
    private final SettingsModelString m_qualityMeasure =
            new SettingsModelString("qualityMeasure", EnumContainer.InformationGain.gini.name());
    private final SettingsModelBoolean m_isClassificationModel = new SettingsModelBoolean("isClassification", true);
    private final SettingsModel[] m_models =
            new SettingsModel[] {m_maxDepthModel, m_maxNoOfBinsModel, m_qualityMeasure, m_isClassificationModel};

    /**
     * Constructor
     */
    public DecisionTreeSettings() {
        super(true);
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
     * @return the {@link InformationGain}
     */
    public InformationGain getQualityMeasure() {
//        return m_qualityMeasure.getStringValue();
        return InformationGain.valueOf(m_qualityMeasure.getStringValue());
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
}