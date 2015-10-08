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
package com.knime.bigdata.spark.port.context;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.swing.JComponent;

import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.port.SparkContextProvider;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkContextPortObject implements PortObject, PortObjectSpec, SparkContextProvider {

    /**
     * Spark context port type.
     */
    public static final PortType TYPE = new PortType(SparkContextPortObject.class);

    /**
     * Spark context type for optional ports.
     */
    public static final PortType TYPE_OPTIONAL = new PortType(SparkContextPortObject.class, true);

    private static final String CONTEXT = "SparkContext";

    private final KNIMESparkContext m_context;

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextPortObject getSpec() {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary() {
        StringBuilder buf = new StringBuilder();
        buf.append("Name: ").append(m_context.getContextName());
        buf.append(" Memory: ").append(m_context.getMemPerNode());
        buf.append(" CPU cores: ").append(m_context.getNumCpuCores());
        return buf.toString();
    }

    /**
     * Creates a Spark context port object.
     * @param context
     */
    public SparkContextPortObject(final KNIMESparkContext context) {
        m_context = context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KNIMESparkContext getContext() {
        return m_context;
    }

    /**
     * Serializer used to save {@link SparkContextPortObject}s.
     */
    public static final class SpecSerializer extends PortObjectSpecSerializer<SparkContextPortObject> {
        @Override
        public void savePortObjectSpec(final SparkContextPortObject portObjectSpec,
            final PortObjectSpecZipOutputStream out)
            throws IOException {
            save(portObjectSpec, out);
        }
        @Override
        public SparkContextPortObject loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
            return load(in);
        }
    }

    /**
     * Serializer used to save {@link SparkContextPortObject}s.
     */
    public static final class ModelSerializer extends PortObjectSerializer<SparkContextPortObject> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void savePortObject(final SparkContextPortObject portObject,
            final PortObjectZipOutputStream out, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
            save(portObject, out);
        }
        /**
         * {@inheritDoc}
         */
        @Override
        public SparkContextPortObject loadPortObject(final PortObjectZipInputStream in,
            final PortObjectSpec spec, final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
            return load(in);
        }
    }

    private static SparkContextPortObject load(final ZipInputStream in) throws IOException {
        try {
            final ZipEntry ze = in.getNextEntry();
            if (!ze.getName().equals(CONTEXT)) {
                throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                        + CONTEXT + "\".");
            }
            final ModelContentRO model = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
            final KNIMESparkContext context = new KNIMESparkContext(model);
            return new SparkContextPortObject(context);
        } catch (InvalidSettingsException ise) {
            throw new IOException(ise);
        }
    }

    private static void save(final SparkContextPortObject portObject, final ZipOutputStream out)
        throws IOException {
        final ModelContent specModel = new ModelContent(CONTEXT);
        final KNIMESparkContext model = portObject.getContext();
        model.save(specModel);
        out.putNextEntry(new ZipEntry(CONTEXT));
        specModel.saveToXML(new NonClosableOutputStream.Zip(out));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return new JComponent[] {new SparkContextConnectionView(m_context)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SparkContextPortObject)) {
            return false;
        }
        final SparkContextPortObject context = (SparkContextPortObject) obj;
        return m_context.equals(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return m_context.hashCode();
    }
}
