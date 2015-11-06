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
 *   Created on 16 ott 2015 by Stefano
 */
package com.knime.bigdata.spark.node.pmml.converter.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;

/**
 *
 * @author Stefano Baghino <stefano.baghino@databiz.it>
 * @author Tobias Koetter, KNIME.com
 */
public class SparkModelPMMLPortObjectFactory {

    private final Map<Class<? extends Serializable>, SparkModel2PMML<?>> m_supportedModels = new HashMap<>();

    private static volatile SparkModelPMMLPortObjectFactory instance;

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static SparkModelPMMLPortObjectFactory getInstance() {
        if (instance == null) {
            synchronized (SparkModelPMMLPortObjectFactory.class) {
                if (instance == null) {
                    instance = new SparkModelPMMLPortObjectFactory();
                }
            }
        }
        return instance;
    }

    private SparkModelPMMLPortObjectFactory() {
        //TODO: Use an extension point for model translators to allow users to write their own translators
        registerConverter(new KMeansModelPMMLPortObjectFactory());
        registerConverter(new LinearRegressionModelPMMLPortObjectFactory());
    }

    /**
     * @param converter the {@link SparkModel2PMML} converter to register
     */
    private void registerConverter(final SparkModel2PMML<? extends Serializable> converter) {
        Class<? extends Serializable> modelClass = converter.getSupportedModelClass();
        if (m_supportedModels.containsKey(modelClass)) {
            throw new IllegalArgumentException(modelClass.getSimpleName()
                + " is already registered.");
        }
        m_supportedModels.put(converter.getSupportedModelClass(), converter);
    }

    /**
     * @param modelClass the model class
     * @return the {@link SparkModel2PMML} converter or <code>null</code> if none exists for the given model class
     */
    private SparkModel2PMML<?> getConverter(final Class<? extends Serializable> modelClass) {
        return m_supportedModels.get(modelClass);
    }

    /**
     * @param port
     * @return The PMMLPortObject ready for usage
     * @throws InvalidSettingsException
     */
    @SuppressWarnings("unchecked")
    public static final PMMLPortObject create(final PortObject port) throws InvalidSettingsException {
        SparkModel<?> model = ((SparkModelPortObject<?>) port).getModel();
        Class<? extends Serializable> modelClass = (Class<? extends Serializable>)model.getModel().getClass();
        @SuppressWarnings("rawtypes")
        final SparkModel2PMML converter = getInstance().getConverter(modelClass);
        if (converter == null) {
            throw new UnsupportedOperationException(modelClass.getSimpleName()
                + " is not supported by the MLlib2PMML node.");
        }
        return converter.convert(model);
    }

}
