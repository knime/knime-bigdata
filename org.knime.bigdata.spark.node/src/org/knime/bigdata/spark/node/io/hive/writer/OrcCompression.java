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
 *   Created on 12.04.2018 by "Mareike Höger, KNIME"
 */
package org.knime.bigdata.spark.node.io.hive.writer;

import java.util.ArrayList;
import java.util.List;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Compression formats supported by the ORC file format.
 *
 * @author "Mareike Hoeger, KNIME"
 */
@SparkClass
public enum OrcCompression {

        /**
         * Zlib compression
         */
        ZLIB,

        /**
         * Snappy Compression
         */
        SNAPPY,

        /**
         * No Compression
         */
        NONE;

    /**
     * @return a list of the values converted to strings
     */
    public static List<String> getStringValues() {
        List<String> compressionsStringList = new ArrayList<>();
        for (OrcCompression compression : values()) {
            compressionsStringList.add(compression.toString());
        }
        return compressionsStringList;
    }

}
