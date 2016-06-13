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
 *   Created on Mar 1, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context;

import java.util.HashMap;

import com.knime.bigdata.spark.core.context.jobserver.JobserverSparkContext;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkContextManager {

    private final static HashMap<SparkContextID, SparkContext> sparkContexts = new HashMap<SparkContextID, SparkContext>();

    private final static SparkContextID DEFAULT_SPARK_CONTEXT_ID = new SparkContextID("default://");

    private static SparkContext defaultSparkContext;

    public static SparkContextID getDefaultSparkContextID() {
        return DEFAULT_SPARK_CONTEXT_ID;
    }

    public synchronized static SparkContext getDefaultSparkContext() {
        if (defaultSparkContext == null) {
            createAndConfigureDefaultSparkContext();
        }
        return defaultSparkContext;
    }

    private static void createAndConfigureDefaultSparkContext() {
        SparkContextConfig defaultConfig = new SparkContextConfig();
        SparkContextID actualDefaultContextID = SparkContextID.fromConnectionDetails(defaultConfig.getJobManagerUrl(), defaultConfig.getContextName());
        defaultSparkContext = new JobserverSparkContext(actualDefaultContextID);
        defaultSparkContext.configure(defaultConfig);
        sparkContexts.put(DEFAULT_SPARK_CONTEXT_ID, defaultSparkContext);
        sparkContexts.put(actualDefaultContextID, defaultSparkContext);
    }

    public synchronized static SparkContext getOrCreateSparkContext(final SparkContextID contextID) {
        SparkContext toReturn = sparkContexts.get(contextID);
        if (toReturn == null) {
            if (contextID.equals(DEFAULT_SPARK_CONTEXT_ID)) {
                createAndConfigureDefaultSparkContext();
                toReturn = defaultSparkContext;
            } else {
                toReturn = new JobserverSparkContext(contextID);
                sparkContexts.put(contextID, toReturn);
            }
        }

        return toReturn;
    }

    public synchronized static void refreshCustomSparkContext(final SparkContext newContext) throws KNIMESparkException {
        checkDefaultContext(newContext.getID());
        sparkContexts.put(newContext.getID(), newContext);
    }

    /**
     * Use with care. This means nodes will not find a context with this ID anymore.
     * @param contextID
     * @throws KNIMESparkException
     */
    public synchronized static void disposeCustomContext(final SparkContextID contextID) throws KNIMESparkException {
        checkDefaultContext(contextID);
        sparkContexts.remove(contextID);
    }

    /**
     * @param contextID the {@link SparkContextID} to check
     * @throws KNIMESparkException if the given {@link SparkContextID} is the default {@link SparkContextID}
     */
    private static void checkDefaultContext(final SparkContextID contextID) throws KNIMESparkException {
        if (contextID.equals(getDefaultSparkContext().getID()) || DEFAULT_SPARK_CONTEXT_ID.equals(contextID)) {
            throw new KNIMESparkException("Cannot modify default Spark context (from KNIME preferences).");
        }
    }

    /**
     * Destroys the {@link SparkContext} with the given {@link SparkContextID} and removes the id
     * from the available contexts. If the context no longer exists the method simply returns.
     * @param contextID the {@link SparkContextID} to destroy
     * @throws KNIMESparkException
     */
    public static void destroyCustomContext(final SparkContextID contextID) throws KNIMESparkException {
        checkDefaultContext(contextID);
        final SparkContext context = sparkContexts.remove(contextID);
        if (context != null) {
            context.destroy();
        }
    }

    public synchronized static void refreshDefaultContext(final boolean destroyOldContext) throws KNIMESparkException {
        if (defaultSparkContext != null) {
            sparkContexts.remove(defaultSparkContext.getID());
            sparkContexts.remove(DEFAULT_SPARK_CONTEXT_ID);

            if (destroyOldContext) {
                defaultSparkContext.ensureDestroyed();
            }

            defaultSparkContext = null;
        }
        createAndConfigureDefaultSparkContext();
    }
}
