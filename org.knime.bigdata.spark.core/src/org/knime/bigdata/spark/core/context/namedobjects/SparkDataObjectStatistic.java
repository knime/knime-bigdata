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
 *   Created on Apr 29, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.core.context.namedobjects;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Container for Spark data object statistics.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class SparkDataObjectStatistic implements NamedObjectStatistics {
    private static final long serialVersionUID = 1L;

    private int m_numPartitions;

    /**
     * Default constructor.
     *
     * @param numPartitions number of partitions
     */
    public SparkDataObjectStatistic(final int numPartitions) {
        m_numPartitions = numPartitions;
    }

    /**
     * @return the number of partitions
     */
    public int getNumPartitions() {
        return m_numPartitions;
    }
}
