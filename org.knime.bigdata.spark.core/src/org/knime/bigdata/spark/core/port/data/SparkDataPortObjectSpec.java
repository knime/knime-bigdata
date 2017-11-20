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
package org.knime.bigdata.spark.core.port.data;

import java.io.IOException;

import javax.swing.JComponent;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.workflow.DataTableSpecView;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;

/**
 * Spark data port object specification, which is uniquely described by a {@link SparkContextID}, a
 * {@link DataTableSpec} and the version of the KNIME Extension for Apache Spark it was created with.
 *
 * @author Tobias Koetter, KNIME GmbH
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkDataPortObjectSpec extends SparkContextPortObjectSpec {

    /**
     * A serializer for {@link SparkDataPortObjectSpec}s.
     *
     */
    public static final class Serializer extends PortObjectSpecSerializer<SparkDataPortObjectSpec> {
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
     * Creates a new instance. Only for internal use.
     *
     * @param sparkData The {@link SparkDataTable} to wrap.
     */
    SparkDataPortObjectSpec(final SparkDataTable sparkData) {
        super(sparkData.getContextID());
        m_data = sparkData;
    }

    /**
     * Creates a new instance bound with the given data table spec and which is bound to the given Spark context.
     *
     * @param contextID The ID of the underlying Spark context.
     * @param spec The {@link DataTableSpec} of the respective Spark data table.
     */
    public SparkDataPortObjectSpec(final SparkContextID contextID, final DataTableSpec spec) {
        this(new SparkDataTable(contextID, "dummy", spec));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return new JComponent[]{new DataTableSpecView(getTableSpec())};
    }

    /**
     * @return the {@link DataTableSpec} of the underlying Spark data table.
     */
    public DataTableSpec getTableSpec() {
        return m_data.getTableSpec();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final SparkContextID getContextID() {
        return m_data.getContextID();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SparkDataPortObjectSpec)) {
            return false;
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec) obj;
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
