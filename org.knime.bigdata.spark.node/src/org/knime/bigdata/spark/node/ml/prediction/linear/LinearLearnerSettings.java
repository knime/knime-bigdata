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
 */
package org.knime.bigdata.spark.node.ml.prediction.linear;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings class for the Spark Linear learner nodes (ml-classification and ml-regression). Which
 * settings are saved/loaded/validated depends on the provided {@link LinearLearnerMode}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LinearLearnerSettings extends MLlibNodeSettings {

    private final LinearLearnerMode m_mode;

    private final SettingsModelString m_lossFunction =
        new SettingsModelString("lossFunction", MLLinearRegressionLearnerLossFunction.SQUARED_ERROR.name());

    private final SettingsModelInteger m_maxIter =
        new SettingsModelIntegerBounded("maxIterations", 100, 1, Integer.MAX_VALUE);

    private final SettingsModelDouble m_convTolerance =
        new SettingsModelDoubleBounded("convergenceTolerance", 0.000001, 0, Double.MAX_VALUE);

    private final SettingsModelBoolean m_useStandardization = new SettingsModelBoolean("useStandardization", true);

    private final SettingsModelString m_solver =
        new SettingsModelString("solver", MLLinearRegressionLearnerSolver.AUTO.name());

    private final SettingsModelString m_family =
        new SettingsModelString("family", MLLogisticRegressionLearnerFamily.AUTO.name());

    private final SettingsModelBoolean m_fitIntercept = new SettingsModelBoolean("fitIntercept", true);

    private final SettingsModelString m_regularizer =
        new SettingsModelString("regularizer", LinearLearnerRegularizer.NONE.name());

    private final SettingsModelDouble m_regParam =
        new SettingsModelDoubleBounded("regParam", 0.01, 0, Double.MAX_VALUE);

    private final SettingsModelDouble m_elasticNetParam = new SettingsModelDoubleBounded("elasticNetParam", 0.5, 0, 1);

    private final SettingsModelString m_handleInvalid =
            new SettingsModelString("handleInvalid", MLLinearRegressionLearnerHandleInvalid.ERROR.name());

    /**
     * @return loss function to optimize
     */
    public MLLinearRegressionLearnerLossFunction getLossFunction() {
        return MLLinearRegressionLearnerLossFunction.valueOf(m_lossFunction.getStringValue());
    }

    /**
     * @return loss function model
     */
    public SettingsModelString getLossFunctionModel() {
        return m_lossFunction;
    }

    /**
     * @return maximal number of iterations
     */
    public int getMaxIter() {
        return m_maxIter.getIntValue();
    }

    /**
     * @return maximal number of iterations model
     */
    public SettingsModelInteger getMaxIterModel() {
        return m_maxIter;
    }

    /**
     * @return convergence tolerance
     */
    public double getConvergenceTolerance() {
        return m_convTolerance.getDoubleValue();
    }

    /**
     * @return convergence tolerance model
     */
    public SettingsModelDouble getConvergenceToleranceModel() {
        return m_convTolerance;
    }

    /**
     * @return {@code true} if the training features should be standardized before fitting the model
     */
    public boolean useStandardization() {
        return m_useStandardization.getBooleanValue();
    }

    /**
     * @return the training features should be standardized before fitting model
     */
    public SettingsModelBoolean getStandardizationModel() {
        return m_useStandardization;
    }

    /**
     * @return solver algorithm used for optimization
     */
    public MLLinearRegressionLearnerSolver getSolver() {
        return MLLinearRegressionLearnerSolver.valueOf(m_solver.getStringValue());
    }

    /**
     * @return solver algorithm model
     */
    public SettingsModelString getSolderModel() {
        return m_solver;
    }

    /**
     * @return label distribution family
     */
    public MLLogisticRegressionLearnerFamily getFamily() {
        return MLLogisticRegressionLearnerFamily.valueOf(m_family.getStringValue());
    }

    /**
     * @return label distribution family model
     */
    public SettingsModelString getFamilyModel() {
        return m_family;
    }

    /**
     * @return {@code true} should fit the intercept
     */
    public boolean fitIntercept() {
        return m_fitIntercept.getBooleanValue();
    }

    /**
     * @return the should fit the intercept model
     */
    public SettingsModelBoolean getFitInterceptorModel() {
        return m_fitIntercept;
    }

    /**
     * @return the regularization to use
     */
    public LinearLearnerRegularizer getRegularizer() {
        return LinearLearnerRegularizer.valueOf(m_regularizer.getStringValue());
    }

    /**
     * @return the regularization type model
     */
    public SettingsModelString getRegularizerModel() {
        return m_regularizer;
    }

    /**
     * @return the regularization parameter
     */
    public double getRegParam() {
        return m_regParam.getDoubleValue();
    }

    /**
     * @return the regularization parameter model
     */
    public SettingsModelDouble getRegParamModel() {
        return m_regParam;
    }

    /**
     * @return the elastic net parameter
     */
    public double getElasticNetParam() {
        return m_elasticNetParam.getDoubleValue();
    }

    /**
     * @return the elastic net parameter model
     */
    public SettingsModelDouble getElasticNetParamModel() {
        return m_elasticNetParam;
    }

    /**
     * @return handle invalid parameter
     */
    public MLLinearRegressionLearnerHandleInvalid getHandleInvalid() {
        return MLLinearRegressionLearnerHandleInvalid.valueOf(m_handleInvalid.getStringValue());
    }

    /**
     * @return handle invalid parameter model
     */
    public SettingsModelString getHandleInvalidModel() {
        return m_handleInvalid;
    }

    /**
     * Constructor.
     *
     * @param mode Whether this is a settings model for the (deprecated) MLlib learner, or the new ML learner nodes
     */
    public LinearLearnerSettings(final LinearLearnerMode mode) {
        super(true, true);
        m_mode = mode;

        m_regularizer.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                updateEnabledness();
            }
        });
    }

    /**
     * Updates the enabledness of settings that are enabled based on the value of other settings.
     */
    protected void updateEnabledness() {
        switch (getRegularizer()) {
            case NONE:
                m_regParam.setEnabled(false);
                m_elasticNetParam.setEnabled(false);
                break;

            case RIDGE:
            case LASSO:
                m_regParam.setEnabled(true);
                m_elasticNetParam.setEnabled(false);
                break;

            case ELASTIC_NET:
                m_regParam.setEnabled(true);
                m_elasticNetParam.setEnabled(true);
                break;
        }
    }

    /**
     * @param settings the {@link NodeSettingsWO} to write to
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);

        m_maxIter.saveSettingsTo(settings);
        m_convTolerance.saveSettingsTo(settings);
        m_useStandardization.saveSettingsTo(settings);
        m_fitIntercept.saveSettingsTo(settings);
        m_regularizer.saveSettingsTo(settings);
        m_regParam.saveSettingsTo(settings);
        m_elasticNetParam.saveSettingsTo(settings);
        m_handleInvalid.saveSettingsTo(settings);

        if (m_mode == LinearLearnerMode.LINEAR_REGRESSION) {
            m_lossFunction.saveSettingsTo(settings);
            m_solver.saveSettingsTo(settings);
        } else if (m_mode == LinearLearnerMode.LOGISTIC_REGRESSION) {
            m_family.saveSettingsTo(settings);
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);

        m_maxIter.validateSettings(settings);
        m_convTolerance.validateSettings(settings);
        m_useStandardization.validateSettings(settings);
        m_fitIntercept.validateSettings(settings);
        m_regularizer.validateSettings(settings);
        m_regParam.validateSettings(settings);
        m_elasticNetParam.validateSettings(settings);
        m_handleInvalid.validateSettings(settings);

        if (m_mode == LinearLearnerMode.LINEAR_REGRESSION) {
            m_lossFunction.validateSettings(settings);
            m_solver.validateSettings(settings);
        } else if (m_mode == LinearLearnerMode.LOGISTIC_REGRESSION) {
            m_family.validateSettings(settings);
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);

        m_maxIter.loadSettingsFrom(settings);
        m_convTolerance.loadSettingsFrom(settings);
        m_useStandardization.loadSettingsFrom(settings);
        m_fitIntercept.loadSettingsFrom(settings);
        m_regularizer.loadSettingsFrom(settings);
        m_regParam.loadSettingsFrom(settings);
        m_elasticNetParam.loadSettingsFrom(settings);
        m_handleInvalid.loadSettingsFrom(settings);

        if (m_mode == LinearLearnerMode.LINEAR_REGRESSION) {
            m_lossFunction.loadSettingsFrom(settings);
            m_solver.loadSettingsFrom(settings);
        } else if (m_mode == LinearLearnerMode.LOGISTIC_REGRESSION) {
            m_family.loadSettingsFrom(settings);
        }

        updateEnabledness();
    }

    /**
     *
     * @return mode of this settings object.
     */
    protected LinearLearnerMode getMode() {
        return m_mode;
    }
}