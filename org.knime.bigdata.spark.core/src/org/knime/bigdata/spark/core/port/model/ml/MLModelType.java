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
 *   Created on Jun 18, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.port.model.ml;

import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;

/**
 * Model category, such as classification, regression, etc, for {@link MLModel}s.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLModelType {

    private static final String KEY_CATEGORY = "category";

    private static final String KEY_UNIQUE_NAME = "uniqueName";

    /**
     * Model category, such as classification, regression, etc, for {@link MLModel}s.
     *
     * @author Bjoern Lohrmann, KNIME GmbH
     */
    public static enum Category {

            /**
             * Indicates a classification model.
             */
            CLASSIFICATION,

            /**
             * Indicates a regression model.
             */
            REGRESSION,

            /**
             * Indicates a clustering model.
             */
            CLUSTERING;
    }

    private final static Map<String, MLModelType> MODEL_TYPES = new HashMap<>();

    private final Category m_category;

    private final String m_uniqueName;

    /**
     * Private constructor to force model type creation through a factory method.
     *
     * @param category Model category, such as classification, regression, etc.
     * @param uniqueName Globally unique name of the model type.
     */
    private MLModelType(final Category category, final String uniqueName) {
        m_category = category;
        m_uniqueName = uniqueName;
    }

    /**
     * @return the model category, such as classification, regression, etc.
     */
    public Category getCategory() {
        return m_category;
    }

    /**
     * @return the globally unique name of the model type.
     */
    public String getUniqueName() {
        return m_uniqueName;
    }

    /**
     * Provides a {@link MLModelType} instance for the given category and unique name.
     *
     * @param category the model category, such as classification, regression, etc.
     * @param uniqueName the globally unique name of the model type.
     * @return a {@link MLModelType} instance
     * @throws IllegalArgumentException if a model type with the given name was already created with a different
     *             category.
     */
    public synchronized static MLModelType getOrCreate(final Category category, final String uniqueName) {
        MLModelType type = MODEL_TYPES.get(uniqueName);
        if (type != null) {
            if (type.getCategory() != category) {
                throw new IllegalArgumentException(
                    String.format("A Spark ML model of type %s is already registered in the %s category.", uniqueName,
                        category.name()));
            }
        } else {
            type = new MLModelType(category, uniqueName);
            MODEL_TYPES.put(uniqueName, type);
        }

        return type;
    }

    /**
     * Saves this instance to the given {@link ModelContentWO}.
     *
     * @param modelContent To save to.
     */
    public void saveToModelContent(final ModelContentWO modelContent) {
        modelContent.addString(KEY_UNIQUE_NAME, m_uniqueName);
        modelContent.addString(KEY_CATEGORY, m_category.name());
    }

    /**
     * Reads a {@link MLModelType} instance from the given {@link ModelContentRO}.
     *
     * @param modelContent To read from.
     * @return a {@link MLModelType} instance.
     * @throws InvalidSettingsException if something went wrong while reading the model type.
     */
    public static MLModelType readFromModelContent(final ModelContentRO modelContent) throws InvalidSettingsException {
        final Category category = Category.valueOf(modelContent.getString(KEY_CATEGORY));
        return getOrCreate(category, modelContent.getString(KEY_UNIQUE_NAME));
    }
}
