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
 *   Created on Jun 15, 2016 by oole
 */
package com.knime.bigdata.spark.node.mllib.prediction.linear;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

import com.knime.bigdata.spark.core.job.util.EnumContainer;
import com.knime.bigdata.spark.core.job.util.EnumContainer.LinearLossFunction;
import com.knime.bigdata.spark.core.job.util.EnumContainer.LinearRegularizer;
import com.knime.bigdata.spark.core.node.MLlibNodeComponents;

/**
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class LinearMethodsComponents extends MLlibNodeComponents<LinearMethodsSettings>{

    private final DialogComponentNumber m_noOfCorrectionsComponent =
            new DialogComponentNumber(getSettings().getNoOfCorrectionsModel(), "Number of corrections: ", 5, 5);

    private final DialogComponentNumber m_toleranceComponent =
            new DialogComponentNumber(getSettings().getToleranceModel(), "Tolerance: ", 0.001, 5);

    private final DialogComponentNumber m_noOfIterationsComponent = new DialogComponentNumber(getSettings().getNoOfIterationsModel(),
        "Number of iterations: ", 10, 5);

    private final DialogComponentStringSelection m_optimizationMethodComponent =
            new DialogComponentStringSelection(getSettings().getOptimizationMethodModel(), "Optimization method",
                getSettings().getOptimizationMethods());

    private final DialogComponentNumber m_regularizationComponent =
            new DialogComponentNumber(getSettings().getRegularizationModel(), "Regularization: ", 0.005, 5);

    private final DialogComponentStringSelection m_updaterTypeComponent = new DialogComponentStringSelection(
        getSettings().getRegularizerTypeModel(), "Regularizer", EnumContainer.getNames(LinearRegularizer.values()));

    private final DialogComponentBoolean m_validateDataComponent =
            new DialogComponentBoolean(getSettings().getValidateDataModel(), "Validate data");

    private final DialogComponentBoolean m_addInterceptComponent =
            new DialogComponentBoolean(getSettings().getAddInterceptModel(), "Add intercept");

    private final DialogComponentBoolean m_useFeatureScalingComponent =
            new DialogComponentBoolean(getSettings().getUseFeatureScalingModel(), "Use feature scaling");

    private final DialogComponentStringSelection m_gradientTypeComponent = new DialogComponentStringSelection(
        getSettings().getLossFunctionTypeModel(), "Loss function", EnumContainer.getNames(LinearLossFunction.values()));

    private final DialogComponentNumber m_stepSizeComponent =
            new DialogComponentNumber(getSettings().getStepSizeModel(), "Step size: ", 0.001, 5);

    private final DialogComponentNumber m_fractionComponent =
            new DialogComponentNumber(getSettings().getFractionModel(), "Fraction: ", 0.001, 5);

    private final DialogComponent[] m_components =  new DialogComponent[] {m_noOfCorrectionsComponent,
        m_toleranceComponent, m_noOfIterationsComponent, m_optimizationMethodComponent, m_regularizationComponent,
        m_updaterTypeComponent, m_validateDataComponent, m_addInterceptComponent, m_useFeatureScalingComponent,
        m_gradientTypeComponent, m_stepSizeComponent, m_fractionComponent};


    /**
     * Constructor.
     * @param settings the {@link LinearMethodsSettings}
     */
    public LinearMethodsComponents(final LinearMethodsSettings settings) {
        super(settings);
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
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        super.saveSettingsTo(settings);
        for (DialogComponent c: m_components) {
            c.saveSettingsTo(settings);
        }
    }

    /**
     * @return the noOfCorrectionsComponent
     */
    public DialogComponent getNoOfCorrectionsComponent() {
        return m_noOfCorrectionsComponent;
    }

    /**
     * @return the toleranceComponent
     */
    public DialogComponent getToleranceComponent() {
        return m_toleranceComponent;
    }

    /**
     * @return the optimizationMethodComponent
     */
    public DialogComponent getOptimizationMethodComponent() {
        return m_optimizationMethodComponent;
    }

    /**
     * @return the noOfIterationsComponent
     */
    public DialogComponent getNoOfIterationsComponent() {
        return m_noOfIterationsComponent;
    }

    /**
     * @return the regularizationComponent
     */
    public DialogComponent getRegularizationComponent() {
        return m_regularizationComponent;
    }

    /**
     * @return the updaterTypeComponent
     */
    public DialogComponent getUpdaterTypeComponent() {
        return m_updaterTypeComponent;
    }

    /**
     * @return the validateDataComponent
     */
    public DialogComponent getValidateDataComponent() {
        return m_validateDataComponent;
    }

    /**
     * @return the addInterceptComponent
     */
    public DialogComponent getAddInterceptComponent() {
        return m_addInterceptComponent;
    }

    /**
     * @return the useFeatureScalingComponent
     */
    public DialogComponent getUseFeatureScalingComponent() {
        return m_useFeatureScalingComponent;
    }

    /**
     * @return the gradientTypeComponent
     */
    public DialogComponent getGradientTypeComponent() {
        return m_gradientTypeComponent;
    }

    /**
     * @return the stepSizeComponent
     */
    public DialogComponent getStepSizeComponent() {
        return m_stepSizeComponent;
    }

    /**
     * @return the fractionComponent
     */
    public DialogComponent getFractionComponent() {
        return m_fractionComponent;
    }
}
