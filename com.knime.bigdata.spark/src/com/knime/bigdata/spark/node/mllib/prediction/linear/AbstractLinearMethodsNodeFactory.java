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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.linear;

import java.io.Serializable;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

import com.knime.bigdata.spark.jobserver.jobs.AbstractRegularizationJob;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.SparkModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> the MLlib model
 */
public abstract class AbstractLinearMethodsNodeFactory<M extends Serializable>
extends NodeFactory<LinearMethodsNodeModel<M>> {


    private SparkModelInterpreter<SparkModel<M>> m_modelInterpreter;
    private Class<? extends AbstractRegularizationJob> m_jobClassPath;
    private boolean m_supportsLBFGS;

    /**
     * @param modelInterpreter the model interpreter
     * @param jobClassPath the job class path
     * @param supportsLBFGS <code>true</code> if the method supports LBFGS
     *
     */
    protected AbstractLinearMethodsNodeFactory(final SparkModelInterpreter<SparkModel<M>> modelInterpreter,
        final Class<? extends AbstractRegularizationJob> jobClassPath, final boolean supportsLBFGS) {
            m_modelInterpreter = modelInterpreter;
            m_jobClassPath = jobClassPath;
            m_supportsLBFGS = supportsLBFGS;

    }
    /**
     * {@inheritDoc}
     */
    @Override
    public LinearMethodsNodeModel<M> createNodeModel() {
        return new LinearMethodsNodeModel<M>(m_jobClassPath, m_modelInterpreter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<LinearMethodsNodeModel<M>> createNodeView(final int viewIndex,
        final LinearMethodsNodeModel<M> nodeModel) {
        return null;
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
