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
 */
package org.knime.bigdata.spark.node;

import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;

/**
 * Maps spark save modes into KNIME.
 *
 * @see org.apache.spark.sql.SaveMode
 */
public class SparkSaveMode {

    /**
     * ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
     * an exception is expected to be thrown.
     *
     * @since Spark 1.3.0
     */
    public static final SparkSaveMode ERROR_IF_EXISTS = new SparkSaveMode("ErrorIfExists", "Error if output data/table exists.");

    /**
     * Overwrite mode means that when saving a DataFrame to a data source,
     * if data/table already exists, existing data is expected to be overwritten by the contents of
     * the DataFrame.
     *
     * @since Spark 1.3.0
     */
    public static final SparkSaveMode OVERWRITE = new SparkSaveMode("Overwrite", "Overwrite existing data/table.");

    /**
     * Append mode means that when saving a DataFrame to a data source, if data/table already exists,
     * contents of the DataFrame are expected to be appended to existing data.
     *
     * @since Spark 1.3.0
     */
    public static final SparkSaveMode APPEND = new SparkSaveMode("Append", "Append data to existing output file/table.");

    /**
     * Ignore mode means that when saving a DataFrame to a data source, if data already exists,
     * the save operation is expected to not save the contents of the DataFrame and to not
     * change the existing data.
     *
     * @since Spark 1.3.0
     */
    public static final SparkSaveMode IGNORE = new SparkSaveMode("Ignore", "Do nothing if output file/table exists.");

    /** Default mode used in dialogs. */
    public static final SparkSaveMode DEFAULT = ERROR_IF_EXISTS;

    /** All available modes. */
    public static final SparkSaveMode ALL[] = new SparkSaveMode[] { ERROR_IF_EXISTS, OVERWRITE, APPEND, IGNORE };


    private final String m_sparkName;
    private final String m_shortDescription;

    private SparkSaveMode(final String sparkName, final String shortDescription) {
        m_sparkName = sparkName;
        m_shortDescription = shortDescription;
    }

    /** @return Spark SaveMode. */
    public String toSparkKey() { return m_sparkName; }

    /** @return KNIME SparkSaveMode by given Spark SaveMode. */
    public static SparkSaveMode fromSparkKey(final String key) {
        for (SparkSaveMode saveMode : ALL) {
            if (saveMode.toSparkKey().equals(key)) {
                return saveMode;
            }
        }
        return null;
    }

    /**
     * Convert a given {@link FileOverwritePolicy} into a Spark save mode.
     *
     * @param policy policy to convert
     * @return spark save mode as string
     */
    public static String toSparkSaveModeKey(final FileOverwritePolicy policy) { // NOSONAR more than three return statements
        switch (policy) {
            case APPEND:
                return APPEND.toSparkKey();
            case FAIL:
                return ERROR_IF_EXISTS.toSparkKey();
            case IGNORE:
                return IGNORE.toSparkKey();
            case OVERWRITE:
                return OVERWRITE.toSparkKey();
            default:
                throw new IllegalArgumentException("Unknown policy: " + policy);
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other != null && other instanceof SparkSaveMode && ((SparkSaveMode) other).m_sparkName.equals(m_sparkName);
    }

    @Override
    public int hashCode() {
        return m_sparkName.hashCode();
    }

    @Override
    public String toString() {
        return m_shortDescription;
    }
}
