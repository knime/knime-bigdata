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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.mllib.prediction.linear;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractLinearMethodsNodeFactory extends DefaultSparkNodeFactory<LinearMethodsNodeModel> {


    private final String m_modelInterpreter;
    private final String m_jobClassPath;
    private boolean m_supportsLBFGS;

    /**
     * @param modelInterpreter the model interpreter
     * @param jobClassPath the job class path
     * @param supportsLBFGS <code>true</code> if the method supports LBFGS
     *
     */
    protected AbstractLinearMethodsNodeFactory(final String modelInterpreter, final String jobClassPath,
        final boolean supportsLBFGS) {
        super("mining/prediction");
        m_modelInterpreter = modelInterpreter;
        m_jobClassPath = jobClassPath;
        m_supportsLBFGS = supportsLBFGS;

    }
    /**
     * {@inheritDoc}
     */
    @Override
    public LinearMethodsNodeModel createNodeModel() {
        return new LinearMethodsNodeModel(m_jobClassPath, m_modelInterpreter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new LinearMethodsNodeDialog(m_supportsLBFGS);
    }

}
