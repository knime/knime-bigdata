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
 */
package org.knime.bigdata.spark.core.exception;

/**
 * This exception indicates that the cluster no longer exists.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkClusterNotFoundException extends KNIMESparkException {

    private static final long serialVersionUID = 1L;
    private final String m_clusterId;

    /**
     * Default constructor.
     * @param clusterId the unique identifier of the non existing cluster
     */
    public SparkClusterNotFoundException(final String clusterId) {
        super(String.format("Spark cluster %s does not exist.", clusterId));
        m_clusterId = clusterId;
    }

    /**
     * @return the identifier of the non existing cluster
     */
    public String getClusterId() {
        return m_clusterId;
    }
}
