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
import java.io.OutputStream;
import java.io.Serializable;

import org.knime.bigdata.spark.core.port.model.ModelInterpreter;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public interface ModelHelper {

    /**
     * @return the unique name of the model
     */
    public String getModelName();

    /**
     * @param inputStream {@link InputStream} to read from
     * @return the serializable model
     * @throws IOException
     */
    public Serializable loadModel(final InputStream inputStream) throws IOException;

    /**
     * @param outputStream {@link OutputStream} to write to
     * @param model the model to write to the {@link OutputStream}
     * @throws IOException
     */
    public void saveModel(final OutputStream outputStream, final Serializable model) throws IOException;

    /**
     * @param inputStream {@link InputStream} to read from
     * @return the model meta data
     * @throws IOException
     */
    public Serializable loadMetaData(final InputStream inputStream) throws IOException;

    /**
     * @param outputStream {@link OutputStream} to wrtie to
     * @param modelMetadata the model meta data to write
     * @throws IOException
     */
    public void saveModelMetadata(final OutputStream outputStream, final Serializable modelMetadata) throws IOException;

    /**
     * @return the {@link ModelInterpreter} for this model type with the given nane
     * @see #getModelName()
     */
    public ModelInterpreter getModelInterpreter();
}
