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
package com.knime.bigdata.spark.core.port.context;

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

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.port.SparkContextProvider;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkContextPortObjectHelper implements SparkContextProvider {

    /**
     * Key required to load legacy workflows (KNIME Spark Executor <= v1.3)
     */
    private static final String KEY_CONTEXT_LEGACY = "SparkContext";

    /**
     * Key required to load current workflows (KNIME Spark Executor >v1.3)
     */
    private static final String KEY_CONTEXT_ID = "SparkContextID";

    private final SparkContextID m_context;


    /**
     * Constructor.
     * @param contextID the {@link SparkContextID}
     */
    protected SparkContextPortObjectHelper(final SparkContextID contextID) {
        m_context = contextID;
    }


    /**
     * @param in {@link ZipInputStream} to read from
     * @return the {@link SparkContextID}
     * @throws IOException if an exception occurs
     */
    @SuppressWarnings("resource")
    public static SparkContextID load(final ZipInputStream in) throws IOException {
        try {
            final ZipEntry ze = in.getNextEntry();

            if (ze.getName().equals(KEY_CONTEXT_ID)) {
                // Load current workflow (KNIME Spark Executor >v1.3)
                final ModelContentRO model = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
                return SparkContextID.fromConfigRO(model);
            } else if (ze.getName().equals(KEY_CONTEXT_LEGACY)) {
                // Load legacy workflow (KNIME Spark Executor <= v1.3)
                final ModelContentRO model = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
                return SparkContextConfig.createSparkContextIDFromLegacyConfig(model);
            } else {
                throw new IOException(String.format("Key %s does not match expected zip entry name \"%s\" or \"%s\"", ze.getName(), KEY_CONTEXT_LEGACY, KEY_CONTEXT_ID));
            }
        } catch (InvalidSettingsException ise) {
            throw new IOException(ise);
        }
    }

    /**
     * @param contextID the context id
     * @param out {@link ZipOutputStream} to write to
     * @throws IOException if an exception occurred
     */
    @SuppressWarnings("resource")
    public static void save(final SparkContextID contextID, final ZipOutputStream out)
        throws IOException {
        final ModelContent specModel = new ModelContent(KEY_CONTEXT_ID);
        contextID.saveToConfigWO(specModel);
        out.putNextEntry(new ZipEntry(KEY_CONTEXT_ID));
        specModel.saveToXML(new NonClosableOutputStream.Zip(out));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextID getContextID() {
        return m_context;
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