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
 *   Created on 25.08.2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.server;

/**
 * Spark RDD persistence types.
 *
 * @author Tobias Koetter, KNIME.com
 */
public enum PersistenceType {

    MEMORY_ONLY,
    MEMORY_AND_DISK,
    MEMORY_ONLY_SER,
    MEMORY_AND_DISK_SER,
    DISK_ONLY,
    MEMORY_ONLY_2,
    OFF_HEAP;
}
