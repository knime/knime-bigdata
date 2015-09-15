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
 *   Created on 27.05.2015 by koetter
 */
package com.knime.bigdata.spark.util;

import java.util.UUID;

import org.knime.core.node.KNIMEConstants;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public final class SparkIDs {

    /**
     * @return a unique id that can be used for named RDDs
     */
    public static String createRDDID() {
        return UUID.randomUUID().toString();
    }

    /**
     * @return the unique Spark id for this user used as the application id
     */
    public static String getSparkApplicationID() {
        final String knimeInstanceID = KNIMEConstants.getKNIMEInstanceID();
        //strip the first part of the id that contains the -
        return knimeInstanceID.substring(knimeInstanceID.indexOf('-') + 1);
    }
}
