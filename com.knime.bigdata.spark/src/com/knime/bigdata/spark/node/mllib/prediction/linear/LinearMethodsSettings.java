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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.knime.bigdata.spark.jobserver.server.EnumContainer;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.LinearLossFunctionTypeType;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.LinearRegularizerType;
import com.knime.bigdata.spark.node.mllib.MLlibNodeSettings;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LinearMethodsSettings extends MLlibNodeSettings {

    /**Stochastic gradient descent method.*/
    public static final String OPTIMIZATION_METHOD_SGD = "Stochastic gradient descent (SGD)";
    /**Limited-memory BFGS method.*/
    public static final String OPTIMIZATION_METHOD_LBFGS = "Limited-memory BFGS (L-BFGS)";

    private static final String[] OPTIMIZATION_METHODS =
            new String[] {OPTIMIZATION_METHOD_SGD, OPTIMIZATION_METHOD_LBFGS};

    private final SettingsModelInteger m_noOfCorrectionsModel =
            new SettingsModelIntegerBounded("numberOfCorrections", 10, 1, Integer.MAX_VALUE);
    //only available for none SGD
    private final SettingsModelDouble m_toleranceModel = new SettingsModelDoubleBounded("tolerance", 0.001, 0, 1);
    //only available for none SGD
    private final SettingsModelInteger m_noOfIterationsModel =
            new SettingsModelIntegerBounded("numberOfIteration", 100, 1, Integer.MAX_VALUE);
    //See https://spark.apache.org/docs/1.2.1/mllib-optimization.html#Choosing-an-Optimization-Method
    private SettingsModelString m_optimizationMethodModel =
            new SettingsModelString("optimizationMethod", OPTIMIZATION_METHOD_SGD);

    private final SettingsModelDouble m_regularizationModel =
            new SettingsModelDoubleBounded("regularization", 0.01, 0, Double.MAX_VALUE);

    /*This defines the http://spark.apache.org/docs/1.2.1/mllib-linear-methods.html#regularizers */
    private final SettingsModelString m_updaterTypeModel =
            new SettingsModelString("updaterType", LinearRegularizerType.L2.name());

    private final SettingsModelBoolean m_validateDataModel = new SettingsModelBoolean("validateDataBeforeTraining", true);

    private final SettingsModelBoolean m_addInterceptModel = new SettingsModelBoolean("addIntercept", false);

    private final SettingsModelBoolean m_useFeatureScalingModel = new SettingsModelBoolean("useFeatureScaling", false);
    /*This defines the http://spark.apache.org/docs/1.2.1/mllib-linear-methods.html#loss-functions */
    private final SettingsModelString m_gradientTypeModel =
            new SettingsModelString("gradientType", LinearLossFunctionTypeType.Logistic.name());

    private final SettingsModelDouble m_stepSizeModel =
            new SettingsModelDoubleBounded("stepSize", 1.0, 0, Double.MAX_VALUE);
    //only available for SGD
    private final SettingsModelDouble m_fractionModel =
            new SettingsModelDoubleBounded("fraction", 1.0, 0, Double.MAX_VALUE);
    //only available for SGD

    private final SettingsModel[] m_models =
            new SettingsModel[] {m_noOfCorrectionsModel, m_toleranceModel, m_noOfIterationsModel,
        m_optimizationMethodModel, m_regularizationModel, m_updaterTypeModel, m_validateDataModel, m_addInterceptModel,
        m_useFeatureScalingModel, m_gradientTypeModel, m_stepSizeModel, m_fractionModel};

    private final DialogComponentNumber m_noOfCorrectionsComponent =
            new DialogComponentNumber(m_noOfCorrectionsModel, "Number of corrections: ", 5, 5);

    private final DialogComponentNumber m_toleranceComponent =
            new DialogComponentNumber(m_toleranceModel, "Tolerance: ", 0.001, 5);

    private final DialogComponentNumber m_noOfIterationsComponent = new DialogComponentNumber(m_noOfIterationsModel,
        "Number of iterations: ", 10, 5);

    private final DialogComponentStringSelection m_optimizationMethodComponent =
            new DialogComponentStringSelection(m_optimizationMethodModel, "Optimization method",
                OPTIMIZATION_METHODS);

    private final DialogComponentNumber m_regularizationComponent =
            new DialogComponentNumber(m_regularizationModel, "Regularization: ", 0.005, 5);

    private final DialogComponentStringSelection m_updaterTypeComponent = new DialogComponentStringSelection(
        m_updaterTypeModel, "Regularizer", EnumContainer.getNames(LinearRegularizerType.values()));

    private final DialogComponentBoolean m_validateDataComponent =
            new DialogComponentBoolean(m_validateDataModel, "Validate data");

    private final DialogComponentBoolean m_addInterceptComponent =
            new DialogComponentBoolean(m_addInterceptModel, "Add intercept");

    private final DialogComponentBoolean m_useFeatureScalingComponent =
            new DialogComponentBoolean(m_useFeatureScalingModel, "Use feature scaling");

    private final DialogComponentStringSelection m_gradientTypeComponent = new DialogComponentStringSelection(
        m_gradientTypeModel, "Loss function", EnumContainer.getNames(LinearLossFunctionTypeType.values()));

    private final DialogComponentNumber m_stepSizeComponent =
            new DialogComponentNumber(m_stepSizeModel, "Step size: ", 0.001, 5);

    private final DialogComponentNumber m_fractionComponent =
            new DialogComponentNumber(m_fractionModel, "Fraction: ", 0.001, 5);

    private final DialogComponent[] m_components =  new DialogComponent[] {m_noOfCorrectionsComponent,
        m_toleranceComponent, m_noOfIterationsComponent, m_optimizationMethodComponent, m_regularizationComponent,
        m_updaterTypeComponent, m_validateDataComponent, m_addInterceptComponent, m_useFeatureScalingComponent,
        m_gradientTypeComponent, m_stepSizeComponent, m_fractionComponent};

    /**
     * Constructor.
     */
    public LinearMethodsSettings() {
        super(true);
        sanityChecks();
        final boolean useSGD = getUseSGD();
        m_stepSizeModel.setEnabled(useSGD);
        m_fractionModel.setEnabled(useSGD);
        m_toleranceModel.setEnabled(!useSGD);
        m_noOfCorrectionsModel.setEnabled(!useSGD);
        m_optimizationMethodModel.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                final boolean sgd = useSGD(m_optimizationMethodModel.getStringValue());
                m_stepSizeModel.setEnabled(sgd);
                m_stepSizeModel.setEnabled(sgd);
                m_fractionModel.setEnabled(sgd);
                m_toleranceModel.setEnabled(!sgd);
                m_noOfCorrectionsModel.setEnabled(!sgd);
            }
        });
    }

    private void sanityChecks() {
        if (m_models.length != m_components.length) {
            //sanity checks
            throw new IllegalStateException("Unequal models and components");
        }
        Set<Object> o = new HashSet<>();
        for (DialogComponent m : m_components) {
            if (!o.add(m.getModel())) {
                throw new IllegalStateException("Duplicate model usage");
            }
        }
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
        final String gTypeString =
                ((SettingsModelString)m_gradientTypeModel.createCloneWithValidatedValue(settings)).getStringValue();
        final LinearLossFunctionTypeType gType = LinearLossFunctionTypeType.valueOf(gTypeString);
        if (gType == null) {
            throw new InvalidSettingsException("Invalid gradient type selected: " + gTypeString);
        }
        final String uTypeString =
                ((SettingsModelString)m_updaterTypeModel.createCloneWithValidatedValue(settings)).getStringValue();
        final LinearRegularizerType uType = LinearRegularizerType.valueOf(uTypeString);
        if (uType == null) {
            throw new InvalidSettingsException("Invalid updater type selected: " + uTypeString);
        }
        final String oTypeString =
            ((SettingsModelString)m_optimizationMethodModel.createCloneWithValidatedValue(settings)).getStringValue();
        boolean found = false;
        for (final String method : OPTIMIZATION_METHODS) {
            if (method.equals(oTypeString)) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new InvalidSettingsException("Invalid optimization method selected: " + oTypeString);
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
     * @return the noOfCorrections
     */
    public int getNoOfCorrections() {
        return m_noOfCorrectionsModel.getIntValue();
    }

    /**
     * @return the tolerance
     */
    public double getTolerance() {
        return m_toleranceModel.getDoubleValue();
    }

    /**
     * @return the useSGD
     */
    public boolean getUseSGD() {
        return useSGD(m_optimizationMethodModel.getStringValue());
    }

    /**
     * @param method the method name to check
     * @return <code>true</code> if the method is the SGD method
     */
    public static boolean useSGD(final String method) {
        return OPTIMIZATION_METHOD_SGD.equals(method);
    }

    /**
     * @return the number of iterations
     */
    public int getNoOfIterations() {
        return m_noOfIterationsModel.getIntValue();
    }

    /**
     * @return the regularization
     */
    public double getRegularization() {
        return m_regularizationModel.getDoubleValue();
    }

    /**
     * @return the updaterType
     */
    public LinearRegularizerType getUpdaterType() {
        return LinearRegularizerType.valueOf(m_updaterTypeModel.getStringValue());
    }

    /**
     * @return the validateData
     */
    public boolean getValidateData() {
        return m_validateDataModel.getBooleanValue();
    }

    /**
     * @return the addIntercept
     */
    public boolean getAddIntercept() {
        return m_addInterceptModel.getBooleanValue();
    }

    /**
     * @return the useFeatureScaling
     */
    public boolean getUseFeatureScaling() {
        return m_useFeatureScalingModel.getBooleanValue();
    }

    /**
     * @return the gradientType
     */
    public LinearLossFunctionTypeType getGradientType() {
        return LinearLossFunctionTypeType.valueOf(m_gradientTypeModel.getStringValue());
    }

    /**
     * @return the stepSize
     */
    public double getStepSize() {
        return m_stepSizeModel.getDoubleValue();
    }

    /**
     * @return the fraction
     */
    public double getFraction() {
        return m_fractionModel.getDoubleValue();
    }

    /**
     * @return the noOfCorrectionsModel
     */
    public SettingsModelInteger getNoOfCorrectionsModel() {
        return m_noOfCorrectionsModel;
    }

    /**
     * @return the toleranceModel
     */
    public SettingsModelDouble getToleranceModel() {
        return m_toleranceModel;
    }

    /**
     * @return the optimizationMethodModel
     */
    public SettingsModelString getOptimizationMethodModel() {
        return m_optimizationMethodModel;
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
        return m_toleranceModel;
    }

    /**
     * @return the updaterTypeModel
     */
    public SettingsModelString getUpdaterTypeModel() {
        return m_updaterTypeModel;
    }

    /**
     * @return the validateDataModel
     */
    public SettingsModelBoolean getValidateDataModel() {
        return m_validateDataModel;
    }

    /**
     * @return the addInterceptModel
     */
    public SettingsModelBoolean getAddInterceptModel() {
        return m_addInterceptModel;
    }

    /**
     * @return the useFeatureScalingModel
     */
    public SettingsModelBoolean getUseFeatureScalingModel() {
        return m_useFeatureScalingModel;
    }

    /**
     * @return the gradientTypeModel
     */
    public SettingsModelString getGradientTypeModel() {
        return m_gradientTypeModel;
    }

    /**
     * @return the stepSizeModel
     */
    public SettingsModelDouble getStepSizeModel() {
        return m_stepSizeModel;
    }

    /**
     * @return the fractionModel
     */
    public SettingsModelDouble getFractionModel() {
        return m_fractionModel;
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
