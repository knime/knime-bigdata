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
 *   Created on Mar 23, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context.jobserver.request;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;

/**
 * Thrown by implementation of {@link AbstractJobserverRequest#sendInternal()} to indicate that an
 * error has occured, but that it may make sense to try sending the request again. This exception is not meant
 * for general use (which is why it is package private).
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
class RetryableKNIMESparkException extends KNIMESparkException {


    /**
     * @param message
     */
    RetryableKNIMESparkException(final String message) {
        super(message);
    }

    private static final long serialVersionUID = 1L;

}
