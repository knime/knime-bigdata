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
package com.knime.bigdata.spark.node.ml.prediction.linear;

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

import com.knime.bigdata.spark.core.job.util.EnumContainer.RegressionSolver;
import com.knime.bigdata.spark.core.node.MLNodeSettings;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @author Ole Ostergaard, KNIME.com
 */
public class LinearMethodsSettings extends MLNodeSettings {


    // the convergence tolerance of iterations. Smaller value will lead to higher accuracy with the cost of more iterations. Default is 1E-6.
    private final SettingsModelDouble m_toleranceModel = new SettingsModelDoubleBounded("tolerance", 0.001, 0, 1);

    // the maximum number of iterations.  Default is 100.
    private final SettingsModelInteger m_noOfIterationsModel =
            new SettingsModelIntegerBounded("numberOfIteration", 100, 1, Integer.MAX_VALUE);

    // the regularization parameter. Default is 0.0.
    private final SettingsModelDouble m_regularizationModel =
            new SettingsModelDoubleBounded("regularization", 0.01, 0, Double.MAX_VALUE);

    // Set the solver algorithm used for optimization.
    //  In case of linear regression, this can be "l-bfgs", "normal" and "auto".
    //  "l-bfgs" denotes Limited-memory BFGS which is a limited-memory quasi-Newton optimization method.
    //  "normal" denotes using Normal Equation as an analytical solution to the linear regression problem.
    //  The default value is "auto" which means that the solver algorithm is selected automatically.
    private final SettingsModelString m_solverTypeModel =
            new SettingsModelString("solverType", RegressionSolver.auto.name());

    // if we should fit the intercept. Default is true.
    private final SettingsModelBoolean m_addInterceptModel = new SettingsModelBoolean("addIntercept", true);

    //  Whether to standardize the training features before fitting the model.
    // The coefficients of models will be always returned on the original scale, so it will be transparent for users.
    //  Note that with/without standardization, the models should be always converged to the same solution when no regularization is applied.
    //  In R's GLMNET package, the default behavior is true as well. Default is true.
    private final SettingsModelBoolean m_useFeatureScalingModel = new SettingsModelBoolean("useFeatureScaling", true);


    //  the ElasticNet mixing parameter.
    //  For alpha = 0, the penalty is an L2 penalty.
    //  For alpha = 1, it is an L1 penalty.
    //  For alpha in (0,1), the penalty is a combination of L1 and L2.
    //  Default is 0.0 which is an L2 penalty.
    private final SettingsModelDouble m_elasticNetParamModel =
            new SettingsModelDoubleBounded("elasticNetParam", 0.0, 0d, 1d);

    private final SettingsModel[] m_models =
        new SettingsModel[]{m_toleranceModel, m_noOfIterationsModel, m_regularizationModel, m_solverTypeModel,
            m_addInterceptModel, m_useFeatureScalingModel, m_elasticNetParamModel};

    /**
     * Constructor.
     */
    public LinearMethodsSettings() {
        super(true, true);
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
     * @return the tolerance
     */
    public double getTolerance() {
        return m_toleranceModel.getDoubleValue();
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
     * @return the ElasticNetParam
     */
    public double getElasticNetParam() {
        return m_elasticNetParamModel.getDoubleValue();
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
     * @return the elastic net param
     */
    public SettingsModelDouble getElasticNetParamModel() {
        return m_elasticNetParamModel;
    }

    /**
     * @return the regularizationModel
     */
    public SettingsModelDouble getRegularizationModel() {
        return m_regularizationModel;
    }

    /**
     * @return the addInterceptModel
     */
    public SettingsModelString getSolverTypeModel() {
        return m_solverTypeModel;
    }

    /**
     * @return the solver type
     */
    public RegressionSolver getSolverType() {
        return RegressionSolver.valueOf(m_solverTypeModel.getStringValue());
    }
}
