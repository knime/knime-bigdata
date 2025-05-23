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
 *   Created on 22.01.2018 by Oleg Yasnev
 */
package org.knime.bigdata.spark.core.context;

import java.util.Map;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.version.SparkProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Interface for different providers of Spark contexts. Providers can be registered via the respective extension point.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @param <T> Spark context configuration class.
 */
public interface SparkContextProvider<T extends SparkContextConfig> extends SparkProvider {

    /**
     * Creates a new Spark context for the given ID. The context will be in state {@link SparkContextStatus#NEW}.
     *
     * @param contextID The ID of the new context.
     * @return a new context.
     */
    SparkContext<T> createContext(SparkContextID contextID);

    /**
     * Returns the URL scheme for {@link SparkContextID}s that is supported by this provider.
     *
     * @return the scheme as an enum.
     */
    SparkContextIDScheme getSupportedScheme();

    /**
     * Provides a prettier version of the context ID to be used for display purposes.
     *
     * @param contextID The ID to prettify.
     * @return a prettier version of the context ID to be used for display purposes.
     */
    String toPrettyString(SparkContextID contextID);


    /**
     * @return the highest Spark version supported by this Spark context provider.
     */
    SparkVersion getHighestSupportedSparkVersion();

    /**
     * Creates a new testing Spark context ID based on the given map of flow variables. Which flow variables are
     * expected is up to the implementing class.
     *
     * @param flowVariables A map of flow variables used to configure the context. Which flow variables are expected is
     *            up to the implementing class.
     * @return a new Spark context ID based on the given map of flow variables.
     * @noreference This is testing code and its API is subject to change without notice.
     * @see TestflowVariable
     * @throws InvalidSettingsException on invalid settings in flow variables
     */
    public SparkContextID createTestingSparkContextID(final Map<String, FlowVariable> flowVariables)
        throws InvalidSettingsException;

    /**
     * Creates a new Spark context configuration based on the given map of flow variables. Which flow variables are
     * expected is up to the implementing class.
     *
     * @param sparkContextId context ID of the testing spark context.
     * @param flowVariables A map of flow variables used to configure the context. Which flow variables are expected is
     *            up to the implementing class.
     * @param fsConnectionId ID of file system connection to use
     * @return a new Spark context configuration based on the given map of flow variables.
     * @noreference This is testing code and its API is subject to change without notice.
     * @see TestflowVariable
     * @throws InvalidSettingsException on invalid settings in flow variables
     */
    public T createTestingSparkContextConfig(final SparkContextID sparkContextId,
        final Map<String, FlowVariable> flowVariables, final String fsConnectionId) throws InvalidSettingsException;
}
