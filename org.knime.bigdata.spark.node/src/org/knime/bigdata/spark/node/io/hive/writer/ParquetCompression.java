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
 * Compression formats supported by the Parquet file format.
 *
 * @author "Mareike Hoeger, KNIME GmbH, Konstanz, Germany"
 */
@SparkClass
public enum ParquetCompression {

        /**
         * Snappy Compression
         */
        SNAPPY("SNAPPY"),
        /**
         * Gzip Compression
         */
        GZIP("GZIP"),
        /**
         * Uncompressed
         */
        NONE("UNCOMPRESSED");

    private final String m_parquetCompressionCodec;

    ParquetCompression(final String parquetCompressionCodec) {
        m_parquetCompressionCodec = parquetCompressionCodec;
    }

    /**
     * @return a list of the values converted to strings
     */
    public static List<String> getStringValues() {
        List<String> compressionsStringList = new ArrayList<>();
        for (ParquetCompression compression : values()) {
            compressionsStringList.add(compression.toString());
        }
        return compressionsStringList;
    }

    /**
     * @return the Parquet codec name for the compression
     */
    public String getParquetCompressionCodec() {
        return m_parquetCompressionCodec;
    }
}
