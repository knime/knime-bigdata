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
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 * Spark model port object specification implementation. A Spark model could be a learned Spark MLlib model.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkModelPortObjectSpec implements PortObjectSpec {
    /**
     * A serializer for {@link SparkModelPortObjectSpec}s.
     *
     * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
     */
    public static final class Serializer extends PortObjectSpecSerializer<SparkModelPortObjectSpec> {
        @Override
        public SparkModelPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in)
            throws IOException {
            ModelContentRO modelContent = loadModelContent(in);
            return new SparkModelPortObjectSpec(modelContent);
        }

        @Override
        public void savePortObjectSpec(final SparkModelPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {
            saveModelContent(out, portObjectSpec);
        }

        /**
         * Reads the model content from the input stream.
         * @param in an input stream
         * @return the model content containing the spec information
         * @throws IOException if an I/O error occurs
         */
        protected ModelContentRO loadModelContent(final PortObjectSpecZipInputStream in) throws IOException {
            ZipEntry ze = in.getNextEntry();
            if (!ze.getName().equals(SPARK_MODEL)) {
                throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                    + SPARK_MODEL + "\".");
            }
            return ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
        }

        /**
         * Saves the given spec object into the output stream.
         * @param os an output stream
         * @param portObjectSpec the port spec
         * @throws IOException if an I/O error occurs
         */
        protected void saveModelContent(final PortObjectSpecZipOutputStream os,
            final SparkModelPortObjectSpec portObjectSpec) throws IOException {
            os.putNextEntry(new ZipEntry(SPARK_MODEL));
            portObjectSpec.m_model.saveToXML(new NonClosableOutputStream.Zip(os));
        }
    }

    private static final String SPARK_MODEL = "spark_model";
    private final ModelContentRO m_model;

    /**
     * Creates a new spec for a Spark model port.
     *
     * @param type connection model
     */
    public SparkModelPortObjectSpec(final String type) {
        if (type == null) {
            throw new IllegalArgumentException("Spark model must not be null.");
        }
        final ModelContent content = new ModelContent("SparkModel");
        content.addString("type", type);
        m_model = content;
    }

    /**
     * @param model
     */
    public SparkModelPortObjectSpec(final ModelContentRO model) {
        m_model = model;
    }

    /**
     * returns the actual model content
     *
     * @return a model content
     */
    protected ModelContentRO getConnectionModel() {
        return m_model;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return new JComponent[]{new SparkModelSpecView(this)};
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
        SparkModelPortObjectSpec spec = (SparkModelPortObjectSpec)obj;
        return m_model.equals(spec.m_model);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return m_model.hashCode();
    }

    /**
     * @return the type of the model
     */
    public String getType() {
        return m_model.getString("type", "none");
    }
}
