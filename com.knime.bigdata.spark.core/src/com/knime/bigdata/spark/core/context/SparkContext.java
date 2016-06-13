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
 *   Created on Mar 2, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context;

import com.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Superclass for all Spark context implementations.
 *
 * NOTE: Implementations must be thread-safe.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public abstract class SparkContext implements JobController, NamedObjectsController {

    public enum SparkContextStatus {
            NEW, CONFIGURED, OPEN
    }

    public abstract SparkContextStatus getStatus();

    public synchronized void ensureOpened() throws KNIMESparkException {
        switch (getStatus()) {
            case NEW:
                throw new KNIMESparkException("Spark context needs to be configured before opening.");
            case CONFIGURED:
                open();
                break;
            case OPEN:
                // all is good
                break;
        }
    }

    public synchronized void ensureDestroyed() throws KNIMESparkException {
        switch (getStatus()) {
            case CONFIGURED:
            case NEW:
                // all is good
                break;
            case OPEN:
                destroy();
                break;
        }
    }

    public abstract void configure(SparkContextConfig config);

    /**
     * Determines whether it is safe to apply the given config to the current context, without destroying it first. Reconfiguring a context is
     * possible under certain circumstances. It is never possible to change the {@link SparkContextID} of an existing
     * context, but especially for new/configured contexts, it is still possible to change every other setting. For contexts
     * that are already open, it depends on the actual settings, e.g. it may not be possible to change the {@link SparkVersion}
     * without destroying the remote context first, but it may be possible to change the job timeout.
     *
     * @param config A new configuration
     * @return true if it is possible to (re)configure this context with the given config, false otherwise.
     */
    public abstract boolean canReconfigure(SparkContextConfig config);

    public abstract void open() throws KNIMESparkException;

    public abstract void destroy() throws KNIMESparkException;

    public abstract SparkContextConfig getConfiguration();

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
