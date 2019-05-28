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
 *   Created on May 27, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.model;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MissingNamedModelException extends KNIMESparkException {

    private static final long serialVersionUID = -2109768361843474755L;

    private final String m_namedModelId;

    /**
     * @param namedModelId The key/ID of the named model that is missing in the Spark context.
     */
    public MissingNamedModelException(final String namedModelId) {
        super("Missing named model with ID " + namedModelId);
        m_namedModelId = namedModelId;
    }

    /**
     * @return the namedModelId
     */
    public String getNamedModelId() {
        return m_namedModelId;
    }
}
