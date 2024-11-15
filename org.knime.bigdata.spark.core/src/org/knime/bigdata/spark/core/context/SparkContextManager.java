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
 *   Created on Mar 1, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context;

import java.util.HashMap;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 * Manager class to get and destroy Spark contexts. This class is to be used by nodes to obtain instances of a Spark
 * context.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkContextManager {

    @SuppressWarnings("rawtypes")
    private final static HashMap<SparkContextID, SparkContext> sparkContexts = new HashMap<>();

    /**
     * This method always returns a Spark context for the given ID. The context may be in any of the possible states,
     * unless it has been configured or opened beforehand.
     *
     * @param contextID The {@link SparkContextID} of the context.
     * @return a Spark context, never null.
     */
    @SuppressWarnings("unchecked")
    public synchronized static <T extends SparkContextConfig> SparkContext<T>
        getOrCreateSparkContext(final SparkContextID contextID) {
        SparkContext<T> toReturn = sparkContexts.get(contextID);
        if (toReturn == null) {
            SparkContextProvider<?> provider =
                SparkContextProviderRegistry.getSparkContextProvider(contextID.getScheme());
            if (provider == null) {
                throw new IllegalArgumentException(
                    "No Spark context provider found for the scheme " + contextID.getScheme());
            }
            toReturn = (SparkContext<T>)provider.createContext(contextID);
            sparkContexts.put(contextID, toReturn);
        }

        return toReturn;
    }

    /**
     * Convenience method that ensures that the {@link SparkContext} with the given {@link SparkContextID} is destroyed,
     * if the ID is known.
     *
     * @param contextID The {@link SparkContextID} to destroy.
     * @throws KNIMESparkException Thrown if anything went wrong while destroying an existing remote Spark context.
     */
    public static void ensureSparkContextDestroyed(final SparkContextID contextID) throws KNIMESparkException {
        final SparkContext<?> context;

        synchronized (SparkContextManager.class) {
            context = sparkContexts.get(contextID); // do not remove the context from sparkContexts
        }

        if (context != null) {
            context.ensureDestroyed();
        }
    }

    /**
     * Ensures that the {@link SparkContext} with the given {@link SparkContextID} is destroyed and deletes any
     * references to it. Use this with caution, because {@link #getOrCreateSparkContext(SparkContextID)} will from then
     * on return a new (unconfigured) Spark context which cannot be used by normal Spark nodes anymore. Use this only if
     * the given Spark context ID is guaranteed to never be used again.
     *
     * @param sparkContextId Identifies the Spark context to destroy and dispose.
     * @throws KNIMESparkException Thrown if anything went wrong while destroying an existing remote Spark context.
     */
    public static void disposeSparkContext(final SparkContextID sparkContextId) throws KNIMESparkException {
        try {
            ensureSparkContextDestroyed(sparkContextId);
        } finally {
            synchronized (SparkContextManager.class) {
                sparkContexts.remove(sparkContextId);
            }
        }
    }
}
