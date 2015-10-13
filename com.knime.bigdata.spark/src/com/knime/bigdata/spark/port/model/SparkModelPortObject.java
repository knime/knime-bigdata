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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.port.model;

import java.io.IOException;
import java.io.Serializable;

import javax.swing.JComponent;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M>
 */
public class SparkModelPortObject<M extends Serializable> implements PortObject {
    //TODO: Should extendSparkContextPortObject
    /**
     * Database port type.
     */
    public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(SparkModelPortObject.class);

    /**
     * Database type for optional ports.
     */
    public static final PortType TYPE_OPTIONAL =
        PortTypeRegistry.getInstance().getPortType(SparkModelPortObject.class, true);

    /**
     * The spec for this port object.
     */
    private final SparkModelPortObjectSpec m_spec;

    private final SparkModel<M> m_model;

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkModelPortObjectSpec getSpec() {
        return m_spec;
    }

    /**
     * Creates a new database port object.
     * @param model
     */
    public SparkModelPortObject(final SparkModel<M> model) {
        m_model = model;
        m_spec = model.getSpec();
    }

    /**
     * @return the model
     */
    public SparkModel<M> getModel() {
        return m_model;
    }

    /**
     * Serializer used to save {@link SparkModelPortObject}s.
     */
    @SuppressWarnings("rawtypes")
    public static final class Serializer extends PortObjectSerializer<SparkModelPortObject> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void savePortObject(final SparkModelPortObject portObject,
            final PortObjectZipOutputStream out, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
            SparkModel model = portObject.getModel();
            model.write(exec, out);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SparkModelPortObject loadPortObject(final PortObjectZipInputStream in,
            final PortObjectSpec spec, final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
            SparkModel<Serializable> model = new SparkModel<>(exec, in);
            return new SparkModelPortObject<>(model);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary() {
        return m_model.getSummary();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return m_model.getViews();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SparkModelPortObject)) {
            return false;
        }
        SparkModelPortObject<?> dbPort = (SparkModelPortObject<?>) obj;
        return m_spec.equals(dbPort.m_spec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return m_spec.hashCode();
    }

}
