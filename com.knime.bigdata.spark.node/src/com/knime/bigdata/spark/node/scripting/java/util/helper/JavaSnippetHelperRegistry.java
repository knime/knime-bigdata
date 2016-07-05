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
 *   Created on May 6, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java.util.helper;

import java.util.HashMap;
import java.util.Map;

import com.knime.bigdata.spark.core.version.SparkProviderRegistry;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Extension point registry for {@link JavaSnippetHelper}s.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JavaSnippetHelperRegistry extends SparkProviderRegistry<JavaSnippetHelper> {

    /** The id of the converter extension point. */
    private static final String EXT_POINT_ID = "com.knime.bigdata.spark.node.JavaSnippetHelper";

    /**The attribute of the extension point.*/
    private static final String EXT_POINT_ATTR_DF = "HelperClass";

    private static JavaSnippetHelperRegistry instance;

    private final Map<SparkVersion, JavaSnippetHelper> m_helperMap = new HashMap<>();

    private JavaSnippetHelperRegistry() {
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static synchronized JavaSnippetHelperRegistry getInstance() {
        if (instance == null) {
            instance = new JavaSnippetHelperRegistry();
            instance.registerExtensions(EXT_POINT_ID, EXT_POINT_ATTR_DF);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addProvider(final JavaSnippetHelper sparkProvider) {

        for (SparkVersion sparkVersion : sparkProvider.getSupportedSparkVersions()) {
            if (m_helperMap.containsKey(sparkVersion)) {
                throw new IllegalArgumentException(String.format("Extension %s is already registered as %s for Spark version %s",
                    m_helperMap.get(sparkVersion).getClass().getName(),
                    JavaSnippetHelper.class.getSimpleName(),
                    sparkVersion.getLabel()));
            }

            m_helperMap.put(sparkVersion, sparkProvider);
        }
    }

    /**
     * @param sparkVersion
     * @return the {@link JavaSnippetHelper} registered for the given Spark version.
     */
    public synchronized JavaSnippetHelper getHelper(final SparkVersion sparkVersion) {
        return m_helperMap.get(sparkVersion);
    }
}
