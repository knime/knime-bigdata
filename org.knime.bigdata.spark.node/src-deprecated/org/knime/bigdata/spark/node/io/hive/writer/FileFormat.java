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

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Available file formats in Hive and Impala.
 *
 * @author "Mareike Hoeger, KNIME GmbH, Konstanz, Germany"
 */
@SparkClass
public enum FileFormat {

        /**
         * Apache ORC format
         */
        ORC(true, false, "ORC"),
        /**
         * Apache Parquet format
         */
        PARQUET(true, true, "Parquet"),
        /**
         * Apache Avro format
         */
        AVRO(true, true, "Avro"),
        /**
         * Plain Textfile
         */
        TEXTFILE(true, true, "Textfile"),
        /**
         * Use the default settings
         */
        CLUSTER_DEFAULT(true, true, "<<Cluster default>>");

    private final boolean m_isImpalaCompatible;

    private final boolean m_isHiveCompatible;

    private final String m_dialogString;

    private FileFormat(final boolean isHiveCompatible, final boolean isImpalaCompatible, final String dialogString) {
        m_isImpalaCompatible = isImpalaCompatible;
        m_isHiveCompatible = isHiveCompatible;
        m_dialogString = dialogString;
    }

    private boolean isHiveCompatible() {
        return m_isHiveCompatible;
    }

    private boolean isImpalaCompatible() {
        return m_isImpalaCompatible;
    }

    /**
     * @return String representation for NodeDialog
     */
    @Override
    public String toString() {
        return m_dialogString;
    }

    /**
     * Retrieves the FileFormat from the given dialog String
     *
     * @param dialogString the dialog string
     * @return returns the file format enum for the given String
     * @throws IllegalArgumentException if no FileFormat exists with this dialog string
     */
    public static FileFormat fromDialogString(final String dialogString) {
        for (FileFormat format : FileFormat.values()) {
            if (format.toString().equalsIgnoreCase(dialogString)) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown compression file format " + dialogString);
    }

    /**
     * @return an array of all file formats compatible with Hive
     */
    public static FileFormat[] getHiveFormats() {
        ArrayList<FileFormat> fileFormat = new ArrayList<>();
        for (FileFormat format : values()) {
            if (format.isHiveCompatible()) {
                fileFormat.add(format);
            }
        }
        return fileFormat.toArray(new FileFormat[0]);
    }

    /**
     * @return an array of all file formats compatible with Hive
     */
    public static FileFormat[] getImpalaFormats() {
        ArrayList<FileFormat> fileFormat = new ArrayList<>();
        for (FileFormat format : values()) {
            if (format.isImpalaCompatible()) {
                fileFormat.add(format);
            }
        }
        return fileFormat.toArray(new FileFormat[0]);
    }
}
