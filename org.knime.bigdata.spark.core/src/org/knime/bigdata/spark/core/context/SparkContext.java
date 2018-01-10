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
 *   Created on Mar 2, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context;

import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Superclass for all Spark context implementations. Spark context here means a client-local
 * handle to talk to the (actual) remote Spark context inside the cluster.
 *
 * NOTE: Implementations must be thread-safe.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public abstract class SparkContext implements JobController, NamedObjectsController {

    /**
     * Possible context states.
     */
    public enum SparkContextStatus {
            /** The context has an ID but no configuration has been provided yet. */
            NEW,

            /** The context has an ID and a configuration. */
            CONFIGURED,

            /**
             * The context is fully configured and the last time someone checked, there was a remote Spark context
             * running
             */
            OPEN
    }

    /**
     * @return the {@link SparkContextStatus}
     */
    public abstract SparkContextStatus getStatus();

    /**
     * Ensures that the Spark context is open, creating a context if necessary and createRemoteContext is true.
     *
     * @param createRemoteContext If true, a non-existent Spark context will be created. Otherwise, a non-existent Spark
     *            context leads to a {@link KNIMESparkException}.
     * @return true when a new context was created, false, if a a context with the same name already existed in the
     *         cluster.
     *
     * @throws KNIMESparkException Thrown if Spark context was non-existent and createRemoteContext=false, or if
     *             something went wrong while creating a Spark context, or if context was not in state configured.
     */
    public synchronized boolean ensureOpened(final boolean createRemoteContext) throws KNIMESparkException {
        switch (getStatus()) {
            case NEW:
                throw new KNIMESparkException("Spark context needs to be configured before opening.");
            case CONFIGURED:
                return open(createRemoteContext);
            default: // this is actually OPEN
                // all is good, but we did not actually create a new context
                return false;
        }
    }

    /**
     * Ensures that no matching remote Spark context exists, destroying it if necessary.
     *
     * @return true if a remote Spark context was actually destroyed, false if no remote Spark context existed.
     * @throws KNIMESparkException Thrown if anything went wrong while destroying an existing remote Spark context.
     */
    public synchronized boolean ensureDestroyed() throws KNIMESparkException {

        boolean contextWasDestroyed = false;

        switch (getStatus()) {
            case NEW:
                // cannot do anything useful without a configuration
                throw new RuntimeException(
                    String.format("Cannot destroy unconfigured Spark context. This is a bug.", getStatus()));
            case CONFIGURED:
            case OPEN:
                try {
                    destroy();
                    contextWasDestroyed= true;
                } catch (SparkContextNotFoundException e) {
                    // we can safely ignore this
                }
                break;
        }

        return contextWasDestroyed;
    }

    /**
     * Ensures that this context is configured, and returns whether the given configuration was applied or not. When
     * this method returns true, it can be assumed that the given configuration was applied and the context is in state
     * {@link SparkContextStatus#CONFIGURED} or {@link SparkContextStatus#OPEN}. If the method returns false, nothing as
     * changed and the context is still in its previous state.
     *
     * <p>
     * NOTE: It is never possible to change the {@link SparkContextID} of a context. In this case this method does
     * nothing and returns false.
     * </p>
     *
     * <p>
     * <ul>
     * <li>If the current status ({@link #getStatus()}) is {@link SparkContextStatus#NEW}, the given configuration will
     * be applied and the context will be {@link SparkContextStatus#CONFIGURED}.</li>
     * <li>If the current status ({@link #getStatus()}) is {@link SparkContextStatus#CONFIGURED}, the given
     * configuration will only be applied if overwriteExistingConfig=true. Otherwise this method does nothing and
     * returns false. Either way, the context stays {@link SparkContextStatus#CONFIGURED}.</li>
     * <li>If the current status ({@link #getStatus()}) is {@link SparkContextStatus#OPEN}, then the given configuration
     * will only be applied if overwriteExistingConfig=true, otherwise this method does nothing and returns false.
     * Moreover, some configuration changes may require destroying a remote context in order to be applied. If this is
     * the case, this method only destroys the remote context if destroyIfNecessary=true, otherwise it does nothing and
     * returns false. If the configuration was applied the context will usually be in state
     * {@link SparkContextStatus#CONFIGURED}, however if the given configuration was identical to the current one, the
     * context remains {@link SparkContextStatus#OPEN}.</li>
     * </p>
     *
     * @param config The new configuration to apply.
     * @param overwriteExistingConfig Whether a pre-existing context configuration should be overwritten.
     * @param destroyIfNecessary Whether an existing remote context shall be destroyed (if necessary required to apply
     *            the new configuration).
     * @return true if the given configuration was successfully applied, false otherwise.
     * @throws KNIMESparkException If something went wrong while destroying the context (only happens if
     *             destroyIfNecessary=true).
     */
    public abstract boolean ensureConfigured(SparkContextConfig config, boolean overwriteExistingConfig,
        boolean destroyIfNecessary) throws KNIMESparkException;

    /**
     * This method equivalent to invoking {@link #ensureConfigured(SparkContextConfig, boolean, boolean)} with the
     * boolean destroyIfNecessary=false.
     *
     * @param config The new configuration to apply.
     * @param overwriteExistingConfig Whether a pre-existing context config should be overwritten.
     * @return true the given config was successfully applied, false otherwise.
     */
    public abstract boolean ensureConfigured(SparkContextConfig config, boolean overwriteExistingConfig);

    /**
     * Opens the Spark context, creating a a new remote context if necessary and createRemoteContext is true.
     *
     * @param createRemoteContext If true, a non-existent Spark context will be created. Otherwise, a non-existent Spark
     *            context leads to a {@link SparkContextNotFoundException}. Setting this parameter to false is useful in
     *            situations where you want to sync the status (see {@link #getStatus()}) to (re)attach to an existing
     *            remote Spark context, but not necessarily create a new one.
     * @return true when a new context was created, false, if a a context with the same name already existed in the
     *         cluster.
     *
     * @throws KNIMESparkException Thrown if something went wrong while creating a Spark context, or if context was not
     *             in state configured.
     * @throws SparkContextNotFoundException Thrown if Spark context was non-existent and createRemoteContext=false
     */
    protected abstract boolean open(final boolean createRemoteContext) throws KNIMESparkException, SparkContextNotFoundException;

    /**
     * Destroys the Spark context within the cluster and frees up all resources.
     *
     * @throws SparkContextNotFoundException Thrown if no remote Spark context actually existed.
     * @throws KNIMESparkException Thrown if anything else went wrong while destroying the remote Spark context.
     */
    protected abstract void destroy() throws KNIMESparkException;

    /**
     * @return the {@link SparkContextConfig}
     */
    public abstract SparkContextConfig getConfiguration();

    /**
     * @return the {@link SparkContextID}
     */
    public abstract SparkContextID getID();

    /**
     * @return A HTML description of this context without HTML and BODY tags
     */
    public abstract String getHTMLDescription();

    /**
     * @return the Spark version supported by this context.
     */
    public abstract SparkVersion getSparkVersion();

}
