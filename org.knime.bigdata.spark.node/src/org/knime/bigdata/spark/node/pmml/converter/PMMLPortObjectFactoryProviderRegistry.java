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
 *   Created on 16 ott 2015 by Stefano
 */
package org.knime.bigdata.spark.node.pmml.converter;

import org.knime.bigdata.spark.core.port.model.MLlibModel;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.bigdata.spark.core.version.DefaultSparkProviderRegistry;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;

/**
 * Registry of all {@link PMMLPortObjectFactory} converter implementations across all {@link SparkVersion}s.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLPortObjectFactoryProviderRegistry
    extends DefaultSparkProviderRegistry<String, PMMLPortObjectFactory, PMMLPortObjectFactoryProvider> {

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "org.knime.bigdata.spark.node.PMMLPortObjectFactoryProvider";

    private static PMMLPortObjectFactoryProviderRegistry instance;

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public synchronized static PMMLPortObjectFactoryProviderRegistry getInstance() {
        if (instance == null) {
            instance = new PMMLPortObjectFactoryProviderRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    /**
     * @param port
     * @return The PMMLPortObject ready for usage
     * @throws InvalidSettingsException
     */
    public static final PMMLPortObject create(final SparkModelPortObject port) throws InvalidSettingsException {
        final MLlibModel model = port.getModel();
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
