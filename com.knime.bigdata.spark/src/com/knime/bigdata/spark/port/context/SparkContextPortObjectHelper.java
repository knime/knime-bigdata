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
 *   Created on 04.10.2015 by koetter
 */
package com.knime.bigdata.spark.port.context;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.swing.JComponent;

import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;

import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.port.SparkContextProvider;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkContextPortObjectHelper implements SparkContextProvider {

    private static final String CONTEXT = "SparkContext";

    private final KNIMESparkContext m_context;

    /**
     * @param in {@link ZipInputStream} to read from
     * @return the {@link KnimeContext}
     * @throws IOException if an exception occurs
     */
    public static KNIMESparkContext load(final ZipInputStream in) throws IOException {
        try {
            final ZipEntry ze = in.getNextEntry();
            if (!ze.getName().equals(CONTEXT)) {
                throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                        + CONTEXT + "\".");
            }
            final ModelContentRO model = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
            final KNIMESparkContext context = new KNIMESparkContext(model);
            return context;
        } catch (InvalidSettingsException ise) {
            throw new IOException(ise);
        }
    }

    /**
     * @param context {@link KNIMESparkContext} to write to output stream
     * @param out {@link ZipOutputStream} to write to
     * @throws IOException if an exception occured
     */
    public static void save(final KNIMESparkContext context, final ZipOutputStream out)
        throws IOException {
        final ModelContent specModel = new ModelContent(CONTEXT);
        context.save(specModel);
        out.putNextEntry(new ZipEntry(CONTEXT));
        specModel.saveToXML(new NonClosableOutputStream.Zip(out));
    }

    /**
     * @return summary string
     */
    public String getSummary() {
        StringBuilder buf = new StringBuilder();
        buf.append("Name: ").append(m_context.getContextName());
        buf.append(" Memory: ").append(m_context.getMemPerNode());
        buf.append(" CPU cores: ").append(m_context.getNumCpuCores());
        return buf.toString();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public KNIMESparkContext getContext() {
        return m_context;
    }


    /**
     * Constructor.
     * @param context the {@link KNIMESparkContext}
     */
    protected SparkContextPortObjectHelper(final KNIMESparkContext context) {
        m_context = context;
    }

    /**
     * @return port object views
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return m_context.toString();
    }
}
