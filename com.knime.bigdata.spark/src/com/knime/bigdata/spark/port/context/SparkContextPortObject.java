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
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * The SparkContextPortObject should not be used as of yet since we do not support different job servers and the
 * job server does not support multiple contexts. We will remove the deprecated from this class as soon as both
 * is supported.
 */
@Deprecated
public class SparkContextPortObject implements PortObject, PortObjectSpec {

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
        buf.append("Context: ").append(m_context.getContextName());
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
     * @return the model
     */
    public KNIMESparkContext getContext() {
        return m_context;
    }

    /**
     * Serializer used to save {@link SparkContextPortObject}s.
     *
     * @return a new serializer
     */
    public static PortObjectSerializer<SparkContextPortObject> getPortObjectSerializer() {
        return new PortObjectSerializer<SparkContextPortObject>() {
            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("resource")
            @Override
            public void savePortObject(final SparkContextPortObject portObject,
                final PortObjectZipOutputStream out, final ExecutionMonitor exec) throws IOException,
                CanceledExecutionException {
                final ModelContent specModel = new ModelContent(CONTEXT);
                KNIMESparkContext model = portObject.getContext();
                model.save(specModel);
                out.putNextEntry(new ZipEntry(CONTEXT));
                specModel.saveToXML(new NonClosableOutputStream.Zip(out));
            }

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("resource")
            @Override
            public SparkContextPortObject loadPortObject(final PortObjectZipInputStream in,
                final PortObjectSpec spec, final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
                try {
                    ZipEntry ze = in.getNextEntry();
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
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return new JComponent[] {new SparkConextConnectionView(m_context)};
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
