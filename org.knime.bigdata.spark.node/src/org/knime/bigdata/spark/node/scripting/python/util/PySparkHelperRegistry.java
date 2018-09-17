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
 *   Created on 31.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.util;

import java.util.HashMap;
import java.util.Map;

import org.knime.bigdata.spark.core.version.SparkProviderRegistry;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper;

/**
 * Extension point registry for {@link PySparkHelper}s.
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class PySparkHelperRegistry extends SparkProviderRegistry<PySparkHelper> {

    /** The id of the converter extension point. */
    private static final String EXT_POINT_ID = "org.knime.bigdata.spark.node.PySparkHelper";

    /**The attribute of the extension point.*/
    private static final String EXT_POINT_ATTR_DF = "HelperClass";

    private final Map<SparkVersion, PySparkHelper> m_helperMap = new HashMap<>();

    private static PySparkHelperRegistry instance;

    private PySparkHelperRegistry() {
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static synchronized PySparkHelperRegistry getInstance() {
        if (instance == null) {
            instance = new PySparkHelperRegistry();
            instance.registerExtensions(EXT_POINT_ID, EXT_POINT_ATTR_DF);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addProvider(final PySparkHelper provider) {
        for (SparkVersion sparkVersion : provider.getSupportedSparkVersions()) {
            if (m_helperMap.containsKey(sparkVersion)) {
                throw new IllegalArgumentException(
                    String.format("Extension %s is already registered as %s for Spark version %s",
                        m_helperMap.get(sparkVersion).getClass().getName(), JavaSnippetHelper.class.getSimpleName(),
                        sparkVersion.getLabel()));
            }

            m_helperMap.put(sparkVersion, provider);
        }
    }

    /**
     * Returns a {@link PySparkHelper} registered for the given Spark version.
     *
     * @param sparkVersion the Spark version
     * @return the {@link PySparkHelper} registered for the given Spark version.
     */
    public synchronized PySparkHelper getHelper(final SparkVersion sparkVersion) {
        return m_helperMap.get(sparkVersion);
    }

    /**
     * Checks whether the given spark version is supported
     *
     * @param sparkVersion the Spark version
     * @return true if this registry supports given spark version
     */
    public synchronized boolean supportsVersion(final SparkVersion sparkVersion) {
        return m_helperMap.containsKey(sparkVersion);
    }


}
