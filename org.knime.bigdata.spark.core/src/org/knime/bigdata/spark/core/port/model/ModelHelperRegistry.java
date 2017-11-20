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
 *   Created on 05.07.2015 by koetter
 */
package org.knime.bigdata.spark.core.port.model;

import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.model.ModelHelper;
import org.knime.bigdata.spark.core.model.ModelHelperProvider;
import org.knime.bigdata.spark.core.version.DefaultSparkProviderRegistry;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Registry for {@link ModelHelper} that allow the interpretation of Spark models.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class ModelHelperRegistry extends DefaultSparkProviderRegistry<String, ModelHelper, ModelHelperProvider> {

    /** The id of the extension point. */
    public static final String EXT_POINT_ID = "org.knime.bigdata.spark.core.ModelHelperProvider";

    private static ModelHelperRegistry instance;

    private ModelHelperRegistry() {}

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public synchronized static ModelHelperRegistry getInstance() {
        if (instance == null) {
            instance = new ModelHelperRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getElementID(final ModelHelper e) {
        return e.getModelName();
    }

    /**
     * @param modelName the unique model name
     * @param sparkVersion Spark version
     * @return the corresponding {@link ModelHelper}
     * @throws MissingSparkModelHelperException if no compatible Spark model helper could be found
     */
    public static ModelHelper getModelHelper(final String modelName, final SparkVersion sparkVersion)
        throws MissingSparkModelHelperException {
        final ModelHelper modelHelper = getInstance().get(modelName, sparkVersion);
        if (modelHelper == null) {
            throw new MissingSparkModelHelperException(
                String.format("No Spark model helper found for model type \"%s\" and Spark version %s", modelName,
                    sparkVersion.getLabel()));
        } else {
            return modelHelper;
        }
    }
}
