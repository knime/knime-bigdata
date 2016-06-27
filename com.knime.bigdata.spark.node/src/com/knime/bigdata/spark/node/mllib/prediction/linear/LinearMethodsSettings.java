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

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.knime.bigdata.spark.core.job.util.EnumContainer.LinearLossFunction;
import com.knime.bigdata.spark.core.job.util.EnumContainer.LinearRegularizer;
import com.knime.bigdata.spark.core.node.MLlibNodeSettings;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @author Ole Ostergaard, KNIME.com
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
    private final SettingsModelString m_regularizerTypeModel =
            new SettingsModelString("updaterType", LinearRegularizer.L2.name());

    private final SettingsModelBoolean m_validateDataModel = new SettingsModelBoolean("validateDataBeforeTraining", true);

    private final SettingsModelBoolean m_addInterceptModel = new SettingsModelBoolean("addIntercept", false);

    private final SettingsModelBoolean m_useFeatureScalingModel = new SettingsModelBoolean("useFeatureScaling", false);
    /*This defines the http://spark.apache.org/docs/1.2.1/mllib-linear-methods.html#loss-functions */
    private final SettingsModelString m_lossFunctionTypeModel =
            new SettingsModelString("gradientType", LinearLossFunction.Logistic.name());

    private final SettingsModelDouble m_stepSizeModel =
            new SettingsModelDoubleBounded("stepSize", 1.0, 0, Double.MAX_VALUE);
    //only available for SGD
    private final SettingsModelDouble m_fractionModel =
            new SettingsModelDoubleBounded("fraction", 1.0, 0, Double.MAX_VALUE);
    //only available for SGD

    private final SettingsModel[] m_models =
            new SettingsModel[] {m_noOfCorrectionsModel, m_toleranceModel, m_noOfIterationsModel,
        m_optimizationMethodModel, m_regularizationModel, m_regularizerTypeModel, m_validateDataModel, m_addInterceptModel,
        m_useFeatureScalingModel, m_lossFunctionTypeModel, m_stepSizeModel, m_fractionModel};



    /**
     * Constructor.
     */
    public LinearMethodsSettings() {
        super(true);

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
                ((SettingsModelString)m_lossFunctionTypeModel.createCloneWithValidatedValue(settings)).getStringValue();
        final LinearLossFunction gType = LinearLossFunction.valueOf(gTypeString);
        if (gType == null) {
            throw new InvalidSettingsException("Invalid gradient type selected: " + gTypeString);
        }
        final String uTypeString =
                ((SettingsModelString)m_regularizerTypeModel.createCloneWithValidatedValue(settings)).getStringValue();
        final LinearRegularizer uType = LinearRegularizer.valueOf(uTypeString);
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
    public LinearRegularizer getRegularizerType() {
        return LinearRegularizer.valueOf(m_regularizerTypeModel.getStringValue());
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
    public LinearLossFunction getLossFuntion() {
        return LinearLossFunction.valueOf(m_lossFunctionTypeModel.getStringValue());
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
    public SettingsModelDouble getToleranceModel() {
        return m_toleranceModel;
    }

    /**
     * @return the updaterTypeModel
     */
    public SettingsModelString getRegularizerTypeModel() {
        return m_regularizerTypeModel;
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
    public SettingsModelString getLossFunctionTypeModel() {
        return m_lossFunctionTypeModel;
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
     * @return the regularizationModel
     */
    public SettingsModelDouble getRegularizationModel() {
        return m_regularizationModel;
    }

    /**
     * @return The optimization methods.
     */
    public String[] getOptimizationMethods() {
        return OPTIMIZATION_METHODS;
    }
}
