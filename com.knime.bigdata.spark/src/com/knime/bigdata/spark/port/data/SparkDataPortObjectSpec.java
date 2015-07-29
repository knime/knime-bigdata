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
package com.knime.bigdata.spark.port.data;

import java.io.IOException;

import javax.swing.JComponent;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.workflow.DataTableSpecView;

import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 * Spark data port object specification.
 * @author Tobias Koetter, KNIME.com
 */
public class SparkDataPortObjectSpec implements PortObjectSpec {
    /**
     * A serializer for {@link SparkDataPortObjectSpec}s.
     *
     * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
     */
    protected static class ConnectionSpecSerializer extends PortObjectSpecSerializer<SparkDataPortObjectSpec> {
        @Override
        public SparkDataPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in)
            throws IOException {
            return new SparkDataPortObjectSpec(new SparkDataTable(in));
        }

        @Override
        public void savePortObjectSpec(final SparkDataPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {
            portObjectSpec.m_data.save(out);
        }
    }


    private final SparkDataTable m_data;

    /**
     * @param sparkData
     */
    SparkDataPortObjectSpec(final SparkDataTable sparkData) {
        m_data = sparkData;
    }

    /**
     * @param context
     * @param spec
     */
    public SparkDataPortObjectSpec(final KNIMESparkContext context, final DataTableSpec spec) {
        this(new SparkDataTable(context, "dummy", spec));
    }

    /**
     * Serializer used to save {@link SparkDataPortObjectSpec}s.
     *
     * @return a new serializer
     */
    public static PortObjectSpecSerializer<SparkDataPortObjectSpec> getPortObjectSpecSerializer() {
        return new ConnectionSpecSerializer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return new JComponent[]{new DataTableSpecView(getTableSpec())};
    }

    /**
     * @return the result {@link DataTableSpec}
     */
    public DataTableSpec getTableSpec() {
        return m_data.getTableSpec();
    }

    /**
     * @return the Spark context
     */
    public final KNIMESparkContext getContext() {
        return m_data.getContext();
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
        SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)obj;
        return m_data.equals(spec.m_data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return m_data.hashCode();
    }
}
