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
 *   Created on Jul 20, 2018 by bjoern
 */
package org.knime.bigdata.spark.core.context.testing;

import java.util.Map;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextProvider;
import org.knime.bigdata.spark.core.context.SparkContextProviderRegistry;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Provides factory methods to create {@link SparkContextConfig} objects for testing purposes.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
public final class TestingSparkContextConfigFactory {

    private TestingSparkContextConfigFactory() {
    }

    /**
     * Creates a {@link SparkContextID} from the given map of flow variables.
     *
     * @param flowVars A map of flow variables that provide the Spark context settings.
     * @return a {@link SparkContextID}
     * @throws InvalidSettingsException
     */
    public static SparkContextID createContextID(final Map<String, FlowVariable> flowVars)
        throws InvalidSettingsException {

        if (!flowVars.containsKey(TestflowVariable.SPARK_CONTEXTIDSCHEME.getName())) {
            throw new InvalidSettingsException("No Spark context provider settings found in flow variables");
        }

        final String idSchemeString = flowVars.get(TestflowVariable.SPARK_CONTEXTIDSCHEME.getName()).getStringValue();
        final SparkContextIDScheme idScheme = SparkContextIDScheme.fromString(idSchemeString);
        final SparkContextProvider<?> provider = getContextProvider(idScheme);
        return provider.createTestingSparkContextID(flowVars);
    }

    /**
     * Creates a {@link SparkContextConfig} from the given map of flow variables.
     *
     * @param sparkContextId ID of testing spark context
     * @param flowVars A map of flow variables that provide the Spark context settings.
     * @param fsConnectionId ID of file system connection to use
     * @return a {@link SparkContextConfig}.
     * @throws InvalidSettingsException
     */
    public static synchronized SparkContextConfig create(final SparkContextID sparkContextId,
        final Map<String, FlowVariable> flowVars, final String fsConnectionId)
        throws InvalidSettingsException {

        final SparkContextProvider<?> provider = getContextProvider(sparkContextId.getScheme());
        return provider.createTestingSparkContextConfig(sparkContextId, flowVars, fsConnectionId);
    }

    private static SparkContextProvider<?> getContextProvider(final SparkContextIDScheme sparkScheme)
        throws InvalidSettingsException {

        final SparkContextProvider<?> provider = SparkContextProviderRegistry.getSparkContextProvider(sparkScheme);
        if (provider == null) {
            throw new InvalidSettingsException("No Spark context provider found for the scheme " + sparkScheme);
        }
        return provider;
    }
}
