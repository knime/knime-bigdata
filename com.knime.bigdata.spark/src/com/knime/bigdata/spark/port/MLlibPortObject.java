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
package com.knime.bigdata.spark.port;

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

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 * @param <M>
 */
public class MLlibPortObject<M extends Serializable> implements PortObject {

    /**
     * The spec for this port object.
     */
    protected final MLlibPortObjectSpec m_spec;

    /**
     * Database port type.
     */
    public static final PortType TYPE = new PortType(MLlibPortObject.class);

    /**
     * Database type for optional ports.
     */
    public static final PortType TYPE_OPTIONAL = new PortType(MLlibPortObject.class, true);

    private MLlibModel<M> m_model;


    /**
     * {@inheritDoc}
     */
    @Override
    public MLlibPortObjectSpec getSpec() {
        return m_spec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary() {
        StringBuilder buf = new StringBuilder();
        buf.append("Type: ").append(m_model.getType());
        return buf.toString();
    }

    /**
     * Creates a new database port object.
     * @param model
     */
    public MLlibPortObject(final MLlibModel<M> model) {
        m_model = model;
        m_spec = model.getSpec();
    }

    /**
     * @return the model
     */
    public MLlibModel<M> getModel() {
        return m_model;
    }

    /**
     * Serializer used to save {@link MLlibPortObject}s.
     *
     * @return a new serializer
     */
    @SuppressWarnings("rawtypes")
    public static PortObjectSerializer<MLlibPortObject> getPortObjectSerializer() {
        return new PortObjectSerializer<MLlibPortObject>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void savePortObject(final MLlibPortObject portObject,
                final PortObjectZipOutputStream out, final ExecutionMonitor exec) throws IOException,
                CanceledExecutionException {
                MLlibModel model = portObject.getModel();
                model.write(exec, out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public MLlibPortObject loadPortObject(final PortObjectZipInputStream in,
                final PortObjectSpec spec, final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
                MLlibModel<Serializable> model = new MLlibModel<>(exec, in);
                return new MLlibPortObject<>(model);
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return m_spec.getViews();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof MLlibPortObject)) {
            return false;
        }
        MLlibPortObject<?> dbPort = (MLlibPortObject<?>) obj;
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
