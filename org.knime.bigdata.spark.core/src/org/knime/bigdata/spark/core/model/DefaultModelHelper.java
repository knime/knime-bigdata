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
 *   Created on Apr 13, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.knime.bigdata.spark.core.util.CustomClassLoadingObjectInputStream;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public abstract class DefaultModelHelper implements ModelHelper {

    private final String m_modelName;

    /**
     * @param modelName the unique name of the model
     */
    public DefaultModelHelper(final String modelName) {
        m_modelName = modelName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return m_modelName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable loadModel(final InputStream inputStream) throws IOException {
        try (ObjectInputStream objectInputStream = new CustomClassLoadingObjectInputStream(inputStream, this.getClass().getClassLoader())) {
            return (Serializable) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not load model, because model class could not be found", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveModel(final OutputStream outputStream, final Serializable model) throws IOException {
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
            objectOutputStream.writeObject(model);
            objectOutputStream.flush();
        }
    }

    @Override
    public Serializable loadMetaData(final InputStream inputStream) throws IOException {
        // default implementation that does nothing
        return null;
    }

    @Override
    public void saveModelMetadata(final OutputStream outputStream, final Serializable modelMetadata) throws IOException {
        // default implementation that does nothing
    }
}
