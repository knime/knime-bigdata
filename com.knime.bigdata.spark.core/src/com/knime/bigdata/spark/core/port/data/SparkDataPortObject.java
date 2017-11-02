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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.core.port.data;

import java.io.IOException;

import javax.swing.JComponent;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.workflow.DataTableSpecView;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.port.context.SparkContextPortObject;

/**
 * {@link PortObject} implementation which holds a reference to a {@link SparkDataTable} object.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkDataPortObject extends SparkContextPortObject {

    /**
     * Database port type.
     */
    @SuppressWarnings("hiding")
    public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(SparkDataPortObject.class);

    /**
     * Database type for optional ports.
     */
    @SuppressWarnings("hiding")
    public static final PortType TYPE_OPTIONAL =
        PortTypeRegistry.getInstance().getPortType(SparkDataPortObject.class, true);


    private final SparkDataTable m_data;

    /**
     * Serializer used to save {@link SparkDataPortObject}s.
     */
    public static final class Serializer extends PortObjectSerializer<SparkDataPortObject> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void savePortObject(final SparkDataPortObject portObject,
            final PortObjectZipOutputStream out, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
            portObject.m_data.save(out);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SparkDataPortObject loadPortObject(final PortObjectZipInputStream in,
            final PortObjectSpec spec, final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
            return new SparkDataPortObject(new SparkDataTable(in));
        }
    }


    /**
     * @param data
     */
    public SparkDataPortObject(final SparkDataTable data) {
        super(data.getContextID());
        m_data = data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkDataPortObjectSpec getSpec() {
        return new SparkDataPortObjectSpec(m_data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary() {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format("ID: %s\n", getData().getID()));
        buf.append(String.format("Columns: %d\n", getTableSpec().getNumColumns()));
        buf.append(String.format("Context: %s\n", super.getSummary()));

        return buf.toString();
    }

    /**
     * @return the model
     */
    public SparkDataTable getData() {
        return m_data;
    }

    /** Basic preview panel implementation. */
    @SuppressWarnings("serial")
    private final class PreviewPanel extends SparkDataPreviewPanel {
        @Override
        public String getName() {
            return "Preview";
        }

        @Override
        protected SparkDataTable prepareDataTable(final ExecutionMonitor exec) throws Exception {
            return m_data;
        }
    }

    @Override
    public JComponent[] getViews() {
        return new JComponent[] {
            new PreviewPanel(),
            new DataTableSpecView(getTableSpec()),
            new SparkDataView(m_data)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SparkDataPortObject)) {
            return false;
        }
        SparkDataPortObject port = (SparkDataPortObject) obj;
        return m_data.equals(port.m_data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return m_data.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextID getContextID() {
        return m_data.getContextID();
    }

    /**
     * @return the unique table name
     */
    public String getTableName() {
        return m_data.getID();
    }

    /**
     * @return the {@link DataTableSpec} of the result table
     */
    public DataTableSpec getTableSpec(){
        return m_data.getTableSpec();
    }
}
