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
 *   Created on Aug 10, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobInput;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.filehandling.core.defaultnodesettings.EnumConfig;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.SettingsModelReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;

/**
 * Settings for the generic to spark node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class GenericDataSource2SparkSettings3 {

    /** Short or long format name in spark. */
    private final String m_format;

    /** Required spark version. */
    private final SparkVersion m_minSparkVersion;

    /** This data source has needs an additional driver jar. */
    private final boolean m_hasDriver;

    /** Required input path. */
    public static final String CFG_INPUT_PATH = "inputPath";

    private final SettingsModelReaderFileChooser m_inputPathChooser;

    /** Upload bundled jar. */
    private static final String CFG_UPLOAD_DRIVER = "uploadDriver";

    private static final boolean DEFAULT_UPLOAD_DRIVER = false;

    private boolean m_uploadDriver = DEFAULT_UPLOAD_DRIVER;

    /**
     * Default constructor. Custom constructors should overwrite {@link #newValidateInstance()} too.
     *
     * @param format - Short or long format name in spark.
     * @param minSparkVersion - Minimum spark version.
     * @param hasDriver - True if this data source has a driver jar.
     * @param portsConfig - Current ports configuration.
     */
    public GenericDataSource2SparkSettings3(final String format, final SparkVersion minSparkVersion,
        final boolean hasDriver, final PortsConfiguration portsConfig) {
        m_format = format;
        m_minSparkVersion = minSparkVersion;
        m_hasDriver = hasDriver;
        m_inputPathChooser = new SettingsModelReaderFileChooser(CFG_INPUT_PATH, portsConfig,
            GenericDataSource2SparkNodeFactory3.FS_INPUT_PORT_GRP_NAME,
            EnumConfig.create(FilterMode.FOLDER, FilterMode.FOLDER));
    }

    /**
     * Constructor used in validation.
     *
     * @param format - Short or long format name in spark.
     * @param minSparkVersion - Minimum spark version.
     * @param hasDriver - True if this data source has a driver jar.
     * @param inputPathChooser - Input path chooser setting smodel
     */
    protected GenericDataSource2SparkSettings3(final String format, final SparkVersion minSparkVersion,
        final boolean hasDriver, final SettingsModelReaderFileChooser inputPathChooser) {
        m_format = format;
        m_minSparkVersion = minSparkVersion;
        m_hasDriver = hasDriver;
        m_inputPathChooser = inputPathChooser;
    }

    /** @return New instance of this settings (overwrite this in custom settings) */
    protected GenericDataSource2SparkSettings3 newValidateInstance() {
        return new GenericDataSource2SparkSettings3(m_format, m_minSparkVersion, m_hasDriver, m_inputPathChooser);
    }

    /** @return Spark format name */
    public String getFormat() {
        return m_format;
    }

    /**
     * @param otherVersion - Version to check
     * @return <code>true</code> if version is compatible
     */
    public boolean isCompatibleSparkVersion(final SparkVersion otherVersion) {
        return m_minSparkVersion.compareTo(otherVersion) <= 0;
    }

    /** @return Minimum required spark version */
    public SparkVersion getMinSparkVersion() {
        return m_minSparkVersion;
    }

    /** @return True if this data source requires additional jar files */
    public boolean hasDriver() {
        return m_hasDriver;
    }

    /**
     * @return Absolute input path model
     */
    public SettingsModelReaderFileChooser getFileChooserModel() {
        return m_inputPathChooser;
    }

    /** @return True if bundled jar should be uploaded */
    public boolean uploadDriver() {
        return m_uploadDriver;
    }

    /** @param uploadDriver - True if bundled jars should be uploaded */
    public void setUploadDriver(final boolean uploadDriver) {
        m_uploadDriver = uploadDriver;
    }

    /** @param settings - Settings to save current settings in */
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_UPLOAD_DRIVER, m_uploadDriver);
    }

    void saveSettingsToModel(final NodeSettingsWO settings) {
        m_inputPathChooser.saveSettingsTo(settings);
        saveSettingsTo(settings);
    }

    void saveSettingsToDialog(final NodeSettingsWO settings) {
        saveSettingsTo(settings);
    }

    /**
     * @param settings - Settings to validate and not load
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_inputPathChooser.validateSettings(settings);

        GenericDataSource2SparkSettings3 tmp = newValidateInstance();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    /**
     * Validate current settings
     *
     * @throws InvalidSettingsException
     */
    public void validateSettings() throws InvalidSettingsException {
        // Nothing to do here, might be overwritten in subclasses
    }

    /**
     * @param settings - Already validated settings to load
     * @throws InvalidSettingsException
     */
    public void loadValidatedSettingsFromModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_inputPathChooser.loadSettingsFrom(settings);
        loadSettings(settings);
    }

    /**
     * @param settings - Already validated settings to load
     */
    public void loadSettingsFromDialog(final NodeSettingsRO settings) {
        loadSettings(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     *
     * @param settings - Settings to load
     */
    protected void loadSettings(final NodeSettingsRO settings) {
        m_uploadDriver = settings.getBoolean(CFG_UPLOAD_DRIVER, DEFAULT_UPLOAD_DRIVER);
    }

    /** @param jobInput - Job input to add custom reader options */
    public void addReaderOptions(final GenericDataSource2SparkJobInput jobInput) {
        // Overwrite this in custom settings
    }
}
