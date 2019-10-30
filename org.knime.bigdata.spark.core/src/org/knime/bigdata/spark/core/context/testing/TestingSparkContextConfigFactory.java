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
public class TestingSparkContextConfigFactory {

    /**
     * Creates a {@link SparkContextConfig} from the given map of flow variables.
     *
     * @param flowVars A map of flow variables that provide the Spark context settings.
     * @return a {@link SparkContextConfig}.
     * @throws InvalidSettingsException
     */
    public synchronized static SparkContextConfig create(final Map<String, FlowVariable> flowVars)
        throws InvalidSettingsException {

        if (!flowVars.containsKey(TestflowVariable.SPARK_CONTEXTIDSCHEME.getName())) {
            throw new IllegalArgumentException("No Spark context provider settings found in flow variables");
        }

        final String idSchemeString = flowVars.get(TestflowVariable.SPARK_CONTEXTIDSCHEME.getName()).getStringValue();
        final SparkContextIDScheme idScheme = SparkContextIDScheme.fromString(idSchemeString);

        final SparkContextProvider<?> provider = SparkContextProviderRegistry.getSparkContextProvider(idScheme);
        if (provider == null) {
            throw new IllegalArgumentException("No Spark context provider found for the scheme " + idSchemeString);
        }

        return provider.createTestingSparkContextConfig(flowVars);
    }
}
