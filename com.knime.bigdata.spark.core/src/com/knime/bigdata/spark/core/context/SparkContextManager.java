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

    private final static HashMap<SparkContextID, SparkContext> sparkContexts =
        new HashMap<>();

    private final static SparkContextID DEFAULT_SPARK_CONTEXT_ID = new SparkContextID("default://");

    private static SparkContext defaultSparkContext;

    /**
     * @return the default Spark context id
     */
    public static SparkContextID getDefaultSparkContextID() {
        return DEFAULT_SPARK_CONTEXT_ID;
    }

    /**
     * @return the default Spark context. Creates a new Spark context with the default context id and the settings
     * from the KNIME preferences page if it does not exists.
     * @see #getDefaultSparkContextID()
     */
    public synchronized static SparkContext getDefaultSparkContext() {
        ensureDefaultContext();
        return defaultSparkContext;
    }

    private static void ensureDefaultContext() {
        if (defaultSparkContext == null) {
            createAndConfigureDefaultSparkContext();
        }
    }

    private static void createAndConfigureDefaultSparkContext() {
        SparkContextConfig defaultConfig = new SparkContextConfig();
        SparkContextID actualDefaultContextID =
            SparkContextID.fromConnectionDetails(defaultConfig.getJobServerUrl(), defaultConfig.getContextName());
        defaultSparkContext = new JobserverSparkContext(actualDefaultContextID);
        defaultSparkContext.configure(defaultConfig);
        sparkContexts.put(DEFAULT_SPARK_CONTEXT_ID, defaultSparkContext);
        sparkContexts.put(actualDefaultContextID, defaultSparkContext);
    }

    /**
     * @param contextID the id of the context
     * @return an existing Spark context or creates a new one with the given id
     */
    public synchronized static SparkContext getOrCreateSparkContext(final SparkContextID contextID) {
        ensureDefaultContext();
        SparkContext toReturn = sparkContexts.get(contextID);
        if (toReturn == null) {
            toReturn = new JobserverSparkContext(contextID);
            sparkContexts.put(contextID, toReturn);
        }

        return toReturn;
    }

    /**
     * Use with care. This means nodes will not find a context with this ID anymore.
     *
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
        ensureDefaultContext();
        if (contextID.equals(getDefaultSparkContext().getID()) || DEFAULT_SPARK_CONTEXT_ID.equals(contextID)) {
            throw new KNIMESparkException("Cannot modify default Spark context (from KNIME preferences).");
        }
    }

    /**
     * Destroys the {@link SparkContext} with the given {@link SparkContextID}.
     * If the context does not exists the method simply returns. The method does not remove the context and the 
     * context id from the list of available contexts but simply destroys the context which changes its state to 
     * configured. To also remove the id and the context from the list call 
     * {@link #disposeCustomContext(SparkContextID)}.
     *
     * @param contextID the {@link SparkContextID} to destroy
     * @throws KNIMESparkException
     */
    public static void destroyCustomContext(final SparkContextID contextID) throws KNIMESparkException {
        checkDefaultContext(contextID);
        final SparkContext context = sparkContexts.get(contextID); // do not remove the context from sparkContexts
        if (context != null) {
            context.destroy();
        }
    }

    /**
     * Tries to reconfigures the default Spark context from the default settings.
     *
     * @param destroyIfNecessary If set to true, the default context will be destroyed, if necessary, i.e. if a setting
     *            has changed that can only be changed by restarting the context.
     * @return true, when reconfiguration was successful, false otherwise.
     * @throws KNIMESparkException When something went wrong while destroying the default context.
     */
    public synchronized static boolean reconfigureDefaultContext(final boolean destroyIfNecessary)
        throws KNIMESparkException {

        final SparkContextConfig newDefaultConfig = new SparkContextConfig();
        final SparkContextID newContextID =
            SparkContextID.fromConnectionDetails(newDefaultConfig.getJobServerUrl(), newDefaultConfig.getContextName());

        // create an entirely new context when the ID changes
        if (defaultSparkContext != null && !newContextID.equals(defaultSparkContext.getID())) {
            sparkContexts.remove(defaultSparkContext.getID());
            sparkContexts.remove(DEFAULT_SPARK_CONTEXT_ID);

            defaultSparkContext = null;
            createAndConfigureDefaultSparkContext();
            return true;
        } else {
            // otherwise try to reconfigure
            return reconfigureContext(DEFAULT_SPARK_CONTEXT_ID, newDefaultConfig, destroyIfNecessary);
        }
    }

    /**
     * Tries to reconfigure the {@link SparkContext} identified by the given ID with the given configuration. Returns
     * false if reconfiguration could not be performed, e.g. if destroyIfNecessary=false but a setting has changed that
     * can only be changed by restarting the context.
     *
     * @param contextID The id of the context to reconfigure.
     * @param newConfig
     * @param destroyIfNecessary When set to true, the context will be destroyed if a setting has changed that can only
     *            be changed by restarting the context.
     * @return true, when reconfiguration was successful, false otherwise.
     * @throws KNIMESparkException When something went wrong while destroying the context.
     */
    public synchronized static boolean reconfigureContext(final SparkContextID contextID,
        final SparkContextConfig newConfig, final boolean destroyIfNecessary) throws KNIMESparkException {
        return getOrCreateSparkContext(contextID).reconfigure(newConfig, destroyIfNecessary);
    }
}
