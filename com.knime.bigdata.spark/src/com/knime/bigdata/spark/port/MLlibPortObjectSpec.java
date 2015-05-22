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
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class MLlibPortObjectSpec implements PortObjectSpec {
    /**
     * A serializer for {@link MLlibPortObjectSpec}s.
     *
     * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
     */
    protected static class ConnectionSpecSerializer extends PortObjectSpecSerializer<MLlibPortObjectSpec> {
        @Override
        public MLlibPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in)
            throws IOException {
            ModelContentRO modelContent = loadModelContent(in);
            return new MLlibPortObjectSpec(modelContent);
        }

        @Override
        public void savePortObjectSpec(final MLlibPortObjectSpec portObjectSpec,
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
            if (!ze.getName().equals(KEY_DATABASE_CONNECTION)) {
                throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                    + KEY_DATABASE_CONNECTION + "\".");
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
            final MLlibPortObjectSpec portObjectSpec) throws IOException {
            os.putNextEntry(new ZipEntry(KEY_DATABASE_CONNECTION));
            portObjectSpec.m_conn.saveToXML(new NonClosableOutputStream.Zip(os));
        }
    }

    private final ModelContentRO m_conn;

    /**
     * Creates a new spec for a database connection port.
     *
     * @param type connection model
     */
    public MLlibPortObjectSpec(final String type) {
        if (type == null) {
            throw new IllegalArgumentException("Database connection model must not be null.");
        }
        final ModelContent content = new ModelContent("MLlib");
        content.addString("type", type);
        m_conn = content;
    }

    /**
     * @param conn
     */
    public MLlibPortObjectSpec(final ModelContentRO conn) {
        m_conn = conn;
    }

    /**
     * returns the actual model content. The actual content is defined by the {@link DatabaseConnectionSettings}
     * class (and its potential subclasses).
     *
     * @return a model content
     */
    protected ModelContentRO getConnectionModel() {
        return m_conn;
    }

    /**
     * Serializer used to save {@link DatabaseConnectionPortObjectSpec}s.
     *
     * @return a new serializer
     */
    public static PortObjectSpecSerializer<MLlibPortObjectSpec> getPortObjectSpecSerializer() {
        return new ConnectionSpecSerializer();
    }

    private static final String KEY_DATABASE_CONNECTION = "database_connection.zip";

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return new JComponent[]{new MLlibConnectionView(m_conn)};
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
        MLlibPortObjectSpec dbSpec = (MLlibPortObjectSpec)obj;
        return m_conn.equals(dbSpec.m_conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return m_conn.hashCode();
    }

    /**
     * @return port type as a string
     */
    public String getType() {
        return m_conn.getString("type", "none");
    }
}
