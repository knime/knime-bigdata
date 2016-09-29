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
 *   Created on Sep 06, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.database.reader;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for JDBC to Spark node.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SuppressWarnings("javadoc")
public class Database2SparkSettings {

    /** Optional driver class to load. */
    private final String CFG_UPLOAD_DRIVER = "uploadDriver";
    private final boolean DEFAULT_UPLOAD_DRIVER = true;
    private boolean m_uploadDriver = DEFAULT_UPLOAD_DRIVER;

    /** Optional partition hints (partitionColumn, lowerBound, upperBound, numPartitions). */
    private final String CFG_PARTITION_COL = "partitionColumn";
    private final String DEFAULT_PARTITON_COL = "";
    private String m_partitionCol = DEFAULT_PARTITON_COL;

    private final String CFG_AUTO_BOUNDS = "autoBounds";
    private final boolean DEFAULT_AUTO_BOUNDS = true;
    private boolean m_autoBounds = DEFAULT_AUTO_BOUNDS;

    private final String CFG_LOWER_BOUND = "lowerBound";
    private final long DEFAULT_LOWER_BOUND = 0;
    private long m_lowerBound = DEFAULT_LOWER_BOUND;

    private final String CFG_UPPER_BOUND = "upperBound";
    private final long DEFAULT_UPPER_BOUND = 1;
    private long m_upperBound = DEFAULT_UPPER_BOUND;

    private final String CFG_NUM_PARTITIONS = "numPartitions";
    private final int DEFAULT_NUM_PARTITONS = 10;
    private int m_numPartitions = DEFAULT_NUM_PARTITONS;

    /** Optional fetch size. */
    private final String CFG_USE_DEFAULT_FETCH_SIZE = "useDefaultmFetchSize";
    private final boolean DEFAULT_USE_DEFAULT_FETCH_SIZE = true;
    private boolean m_useDefaultFetchSize = DEFAULT_USE_DEFAULT_FETCH_SIZE;

    private final String CFG_FETCH_SIZE = "fetchSize";
    private final int DEFAULT_FETCH_SIZE = 100;
    private int m_fetchSize = DEFAULT_FETCH_SIZE;


    public boolean uploadDriver() { return m_uploadDriver; }
    public void setUploadDriver(final boolean uploadDriver) { m_uploadDriver = uploadDriver; }

    public boolean usePartitioning() { return !StringUtils.isBlank(m_partitionCol); }

    public String getPartitionColumn() { return m_partitionCol; }
    public void setPartitionColumn(final String partitionCol) { m_partitionCol = partitionCol; }

    public boolean useAutoBounds() { return m_autoBounds; }
    public void setAutoBounds(final boolean autoBounds) { m_autoBounds = autoBounds; }

    public long getLowerBound() { return m_lowerBound; }
    public void setLowerBound(final long lowerBound) { m_lowerBound = lowerBound; }

    public long getUpperBound() { return m_upperBound; }
    public void setUpperBound(final long upperBound) { m_upperBound = upperBound; }

    public int getNumPartitions() { return m_numPartitions; }
    public void setNumPartitions(final int numPartitions) { m_numPartitions = numPartitions; }

    public boolean useDefaultFetchSize() { return m_useDefaultFetchSize; }
    public void setUseDefaultFetchSize(final boolean useDefaultFetchSize) {  m_useDefaultFetchSize = useDefaultFetchSize; }
    public int getFetchSize() { return m_fetchSize; }
    public void setFetchSize(final int fetchSize) { this.m_fetchSize = fetchSize; }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_UPLOAD_DRIVER, m_uploadDriver);
        settings.addString(CFG_PARTITION_COL, m_partitionCol);
        settings.addBoolean(CFG_AUTO_BOUNDS, m_autoBounds);
        settings.addLong(CFG_LOWER_BOUND, m_lowerBound);
        settings.addLong(CFG_UPPER_BOUND, m_upperBound);
        settings.addInt(CFG_NUM_PARTITIONS, m_numPartitions);
        settings.addBoolean(CFG_USE_DEFAULT_FETCH_SIZE, m_useDefaultFetchSize);
        settings.addInt(CFG_FETCH_SIZE, m_fetchSize);
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        Database2SparkSettings tmp = new Database2SparkSettings();
        tmp.loadSettingsFrom(settings);
        tmp.validateSettings();
    }

    public void validateSettings() throws InvalidSettingsException {
        if (!StringUtils.isBlank(m_partitionCol)) {
            if (!m_autoBounds && m_lowerBound > m_upperBound) {
                throw new InvalidSettingsException("Lower bound has to be lower than upper bound.");
            }

            if (m_numPartitions <= 0) {
                throw new InvalidSettingsException("Number of partitions has to be greater than zero.");
            }
        }

        if (!m_useDefaultFetchSize && m_fetchSize <= 0) {
            throw new InvalidSettingsException("Fetch size has to be greater than zero.");
        }
    }

    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettingsFrom(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) {
        m_uploadDriver = settings.getBoolean(CFG_UPLOAD_DRIVER, DEFAULT_UPLOAD_DRIVER);
        m_partitionCol = settings.getString(CFG_PARTITION_COL, DEFAULT_PARTITON_COL);
        m_autoBounds = settings.getBoolean(CFG_AUTO_BOUNDS, DEFAULT_AUTO_BOUNDS);
        m_lowerBound = settings.getLong(CFG_LOWER_BOUND, DEFAULT_LOWER_BOUND);
        m_upperBound = settings.getLong(CFG_UPPER_BOUND, DEFAULT_UPPER_BOUND);
        m_numPartitions = settings.getInt(CFG_NUM_PARTITIONS, DEFAULT_NUM_PARTITONS);
        m_useDefaultFetchSize = settings.getBoolean(CFG_USE_DEFAULT_FETCH_SIZE, DEFAULT_USE_DEFAULT_FETCH_SIZE);
        m_fetchSize = settings.getInt(CFG_FETCH_SIZE, DEFAULT_FETCH_SIZE);
    }
}
