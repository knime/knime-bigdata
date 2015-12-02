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
 *   Created on 16.07.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

/**
 * indicates mapping type
 *
 * @author dwk
 */
public enum MappingType {
    /**
     * use one map for all columns so that the same value in different columns is mapped to the same number
     */
    GLOBAL,
    /**
     * use a separate map for each column to that the distinct values in each column are always numbered from 1 to N
     */
    COLUMN,
    /**
     * use a binary mapping - one new column for each distinct value
     */
    BINARY;
}
