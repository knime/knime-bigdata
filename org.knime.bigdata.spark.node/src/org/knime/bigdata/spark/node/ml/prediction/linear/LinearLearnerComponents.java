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

import org.knime.bigdata.spark.core.node.MLlibNodeComponents;
import org.knime.bigdata.spark.node.mllib.prediction.linear.LinearMethodsSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;

/**
 * ML-based linear learner dialog components.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LinearLearnerComponents extends MLlibNodeComponents<LinearLearnerSettings> {

    private final DialogComponentButtonGroup m_lossFunction = new DialogComponentButtonGroup(
        getSettings().getLossFunctionModel(), null, false, MLLinearRegressionLearnerLossFunction.values());

    private final DialogComponentNumber m_maxIter =
        new DialogComponentNumber(getSettings().getMaxIterModel(), null, 1, 5);

    private final DialogComponentBoolean m_standardization =
        new DialogComponentBoolean(getSettings().getStandardizationModel(), null);

    private final DialogComponentBoolean m_fitIntercept =
        new DialogComponentBoolean(getSettings().getFitInterceptorModel(), null);

    private final DialogComponentButtonGroup m_regularizer = new DialogComponentButtonGroup(
        getSettings().getRegularizerModel(), null, false, LinearLearnerRegularizer.values());

    private final DialogComponentNumber m_regParam =
        new DialogComponentNumber(getSettings().getRegParamModel(), null, 0.01, 5);

    private final DialogComponentNumber m_elasticNetParam =
        new DialogComponentNumber(getSettings().getElasticNetParamModel(), null, 0.01, 5);

    private final DialogComponentButtonGroup m_solver = new DialogComponentButtonGroup(getSettings().getSolderModel(),
        null, false, MLLinearRegressionLearnerSolver.values());

    private final DialogComponentNumber m_convTol =
        new DialogComponentNumber(getSettings().getConvergenceToleranceModel(), null, 0.000001, 10);

    private final DialogComponent[] m_components = new DialogComponent[]{m_lossFunction, m_maxIter, m_standardization,
        m_fitIntercept, m_regularizer, m_regParam, m_elasticNetParam, m_solver, m_convTol};

    /**
     * Constructor.
     *
     * @param settings the {@link LinearMethodsSettings}
     */
    public LinearLearnerComponents(final LinearLearnerSettings settings) {
        super(settings, settings.getMode() == LinearLearnerMode.LOGISTIC_REGRESSION, false);
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
            c.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        super.saveSettingsTo(settings);
        for (DialogComponent c : m_components) {
            c.saveSettingsTo(settings);
        }
    }

    /**
     * @return loss function dialog component
     */
    public DialogComponentButtonGroup getLossFunctionComponent() {
        return m_lossFunction;
    }

    /**
     * @return maximal iterations dialog component
     */
    public DialogComponentNumber getMaxIterComponent() {
        return m_maxIter;
    }

    /**
     * @return standardize features dialog component
     */
    public DialogComponentBoolean getStandardizationComponent() {
        return m_standardization;
    }

    /**
     * @return fit intercept dialog component
     */
    public DialogComponentBoolean getFitInterceptComponent() {
        return m_fitIntercept;
    }

    /**
     * @return regularizer dialog component
     */
    public DialogComponentButtonGroup getRegularizerComponent() {
        return m_regularizer;
    }

    /**
     * @return regularization parameter dialog component
     */
    public DialogComponentNumber getRegParamComponent() {
        return m_regParam;
    }

    /**
     * @return elastic net dialog component
     */
    public DialogComponentNumber getElasticNetParamComponent() {
        return m_elasticNetParam;
    }

    /**
     * @return solver dialog component
     */
    public DialogComponentButtonGroup getSolverComponent() {
        return m_solver;
    }

    /**
     * @return convergence tolerance dialog component
     */
    public DialogComponentNumber getConvergenceToleranceComponent() {
        return m_convTol;
    }
}
