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
package com.knime.bigdata.spark.node.ml.prediction.linear;

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
import com.knime.bigdata.spark.core.job.util.EnumContainer.RegressionSolver;
import com.knime.bigdata.spark.core.node.MLNodeComponents;

/**
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class LinearMethodsComponents extends MLNodeComponents<LinearMethodsSettings>{

    // the convergence tolerance of iterations. Smaller value will lead to higher accuracy with the cost of more iterations. Default is 1E-6.
    private final DialogComponentNumber m_toleranceComponent =
            new DialogComponentNumber(getSettings().getToleranceModel(), "Tolerance: ", 0.001, 5);

    // the maximum number of iterations.  Default is 100.
    private final DialogComponentNumber m_noOfIterationsComponent = new DialogComponentNumber(getSettings().getNoOfIterationsModel(),
        "Number of iterations: ", 10, 5);

    // the regularization parameter. Default is 0.0.
    private final DialogComponentNumber m_regularizationComponent =
            new DialogComponentNumber(getSettings().getRegularizationModel(), "Regularization: ", 0.005, 5);

    // Set the solver algorithm used for optimization.
    //  In case of linear regression, this can be "l-bfgs", "normal" and "auto".
    //  "l-bfgs" denotes Limited-memory BFGS which is a limited-memory quasi-Newton optimization method.
    //  "normal" denotes using Normal Equation as an analytical solution to the linear regression problem.
    //  The default value is "auto" which means that the solver algorithm is selected automatically.
    private final DialogComponentStringSelection m_solverTypeComponent = new DialogComponentStringSelection(
        getSettings().getSolverTypeModel(), "Solver", EnumContainer.getNames(RegressionSolver.values()));


    // if we should fit the intercept. Default is true.
    private final DialogComponentBoolean m_addInterceptComponent =
            new DialogComponentBoolean(getSettings().getAddInterceptModel(), "Add intercept");

    //  Whether to standardize the training features before fitting the model.
    // The coefficients of models will be always returned on the original scale, so it will be transparent for users.
    //  Note that with/without standardization, the models should be always converged to the same solution when no regularization is applied.
    //  In R's GLMNET package, the default behavior is true as well. Default is true.
    private final DialogComponentBoolean m_useFeatureScalingComponent =
            new DialogComponentBoolean(getSettings().getUseFeatureScalingModel(), "Use feature scaling");

    //  the ElasticNet mixing parameter.
    //  For alpha = 0, the penalty is an L2 penalty.
    //  For alpha = 1, it is an L1 penalty.
    //  For alpha in (0,1), the penalty is a combination of L1 and L2.
    //  Default is 0.0 which is an L2 penalty.
    private final DialogComponentNumber m_elasticNetParamComponent =
            new DialogComponentNumber(getSettings().getElasticNetParamModel(), "Elastic Net Param: ", 0.001, 5);

    private final DialogComponent[] m_components =  new DialogComponent[] {
        m_toleranceComponent, m_noOfIterationsComponent, m_regularizationComponent,
        m_solverTypeComponent, m_addInterceptComponent, m_useFeatureScalingComponent,
        m_elasticNetParamComponent};


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
     * @return the toleranceComponent
     */
    public DialogComponent getToleranceComponent() {
        return m_toleranceComponent;
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
     * @return the elasticNetParamComponent
     */
    public DialogComponent getElasticNetParamComponent() {
        return m_elasticNetParamComponent;
    }

    /**
     * @return the solverTypeComponent
     */
    public DialogComponent getSolverTypeComponent() {
        return m_solverTypeComponent;
    }

}
