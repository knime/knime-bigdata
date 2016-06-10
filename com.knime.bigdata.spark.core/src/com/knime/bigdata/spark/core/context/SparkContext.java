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
 * subclasses must be threadsafe
 *
 * @author bjoern
 */
public abstract class SparkContext implements JobController, NamedObjectsController {

    public enum SparkContextStatus {
        NEW,
        CONFIGURED,
        OPEN
    }

    public abstract SparkContextStatus getStatus();

    public void ensureOpened() throws KNIMESparkException {
        switch(getStatus()) {
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

    public void ensureDestroyed() throws KNIMESparkException {
        switch(getStatus()) {
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
