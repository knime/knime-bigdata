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
package com.knime.bigdata.spark.node.pmml.converter;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.core.port.model.SparkModel;
import com.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.core.version.DefaultSparkProviderRegistry;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Registry of all {@link PMMLPortObjectFactory} converter implementations across all {@link SparkVersion}s.
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLPortObjectFactoryProviderRegistry
    extends DefaultSparkProviderRegistry<String, PMMLPortObjectFactory, PMMLPortObjectFactoryProvider> {

    /**The id of the converter extension point.*/
    public static final String EXT_POINT_ID = "com.knime.bigdata.spark.node.PMMLPortObjectFactoryProvider";

    private static volatile PMMLPortObjectFactoryProviderRegistry instance;

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static PMMLPortObjectFactoryProviderRegistry getInstance() {
        if (instance == null) {
            synchronized (PMMLPortObjectFactoryProviderRegistry.class) {
                if (instance == null) {
                    instance = new PMMLPortObjectFactoryProviderRegistry();
                    instance.registerExtensions(EXT_POINT_ID);
                }
            }
        }
        return instance;
    }

    /**
     * @param port
     * @return The PMMLPortObject ready for usage
     * @throws InvalidSettingsException
     */
    public static final PMMLPortObject create(final SparkModelPortObject port) throws InvalidSettingsException {
        final SparkModel model = port.getModel();
        final PMMLPortObjectFactory converter = getConverter(model.getSparkVersion(), model.getModelName());
        if (converter == null) {
            throw new UnsupportedOperationException("No PMML converter found for model: " + model.getModelName());
        }
        return converter.convert(model);
    }

    /**
     * @param sparkVersion the {@link SparkVersion}
     * @param modelName the unique model name
     * @return the corresponding {@link PMMLPortObjectFactory} or <code>null</code> if none exists
     */
    public static PMMLPortObjectFactory getConverter(final SparkVersion sparkVersion, final String modelName) {
        return getInstance().get(modelName, sparkVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getElementID(final PMMLPortObjectFactory e) {
        return e.getModelName();
    }
}
