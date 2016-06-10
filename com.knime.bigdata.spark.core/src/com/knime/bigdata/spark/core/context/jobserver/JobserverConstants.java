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
 *   Created on Mar 3, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context.jobserver;

import com.knime.bigdata.spark.core.util.SparkIDs;

/**
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class JobserverConstants {


    /**
     * We use the Spark application ID which is based on the KNIME id which is unique per workspace.
     */
    public final static String APP_NAME = SparkIDs.getSparkApplicationID();


    /**
     * path prefix for jobs
     */
    public static final String JOBS_PATH = "/jobs";


    /**
     * path prefix for contexts
     */
    public static final String CONTEXTS_PATH = "/contexts";

    /**
     * path prefix for jars
     */
    public static final String JARS_PATH = "/jars";

    /**
     * Some errors can be handled by retrying an action on the jobserver. This is the maximum number of retries to do.
     */
    public static final int MAX_REQUEST_ATTEMTPS = 3;


    /**
     * path prefix for data
     */
    public static final String DATA_PATH = "/data";


    public static String buildJobPath(final String jobID) {
        return String.format("%s/%s", JOBS_PATH, jobID);
    }

    public static String buildContextPath(final String contextName) {
        return String.format("%s/%s", CONTEXTS_PATH, contextName);
    }

    public static String buildJarPath() {
        return String.format("%s/%s", JARS_PATH, APP_NAME);
    }

    public static String buildDataPath(final String prefixOrFile) {
        return String.format("%s/%s", DATA_PATH, prefixOrFile);
    }

}
