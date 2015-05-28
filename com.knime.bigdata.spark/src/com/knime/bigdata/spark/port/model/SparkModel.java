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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.zip.ZipEntry;

import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;

/**
 * Spark model that encapsulates a learned Spark MLlib model.
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> the model
 */
public class SparkModel<M extends Serializable> {

    private static final String MODEL_ENTRY = "Model";
    private M m_model;
    private String m_type;

    /**
     * @param type model type
     * @param model the model
     */
    public SparkModel(final String type, final M model) {
        m_type = type;
        m_model = model;
    }

    /**
     * @param exec
     * @param in
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public SparkModel(final ExecutionMonitor exec, final PortObjectZipInputStream in)
            throws IOException {
        final ZipEntry type = in.getNextEntry();
        if (!type.getName().equals(MODEL_ENTRY)) {
            throw new IOException("Invalid zip entry");
        }
        try (final ObjectInputStream os = new ObjectInputStream(in);){
            m_type = (String)os.readObject();
            m_model = (M)os.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /**
     * @param exec
     * @param out
     * @throws IOException
     */
    public void write(final ExecutionMonitor exec, final PortObjectZipOutputStream out) throws IOException {
        out.putNextEntry(new ZipEntry(MODEL_ENTRY));
        try (final ObjectOutputStream os = new ObjectOutputStream(out)){
            os.writeObject(getType());
            os.writeObject(getModel());
        }
    }

    /**
     * @return the spec
     */
    public SparkModelPortObjectSpec getSpec() {
        return new SparkModelPortObjectSpec(getType());
    }

    /**
     * @return the type
     */
    public String getType() {
        return m_type;
    }

    /**
     * @return the model
     */
    public M getModel() {
        return m_model;
    }

}
