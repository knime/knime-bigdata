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
 *   Created on 23.04.2018 by "Mareike Höger, KNIME"
 */
package org.knime.bigdata.spark.node.io.database.hive.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.bigdata.spark.node.io.hive.writer.OrcCompression;
import org.knime.bigdata.spark.node.io.hive.writer.ParquetCompression;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * Settings class for the Spark2Hive/Spark2Impala Node
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class DBSpark2HiveSettings {

    private final SettingsModelString m_schema;

    private final SettingsModelString m_tableName;

    private final SettingsModelString m_onTableExistsAction;

    private final SettingsModelString m_fileFormat;

    private final SettingsModelString m_compression;

    /**
     * Creates a Spark2Hive Settings instance with the given default format preset
     *
     * @param defaultFormat the default file format to use
     *
     */
    public DBSpark2HiveSettings(final FileFormat defaultFormat) {
        m_schema = new SettingsModelString("schema", "");
        m_tableName = new SettingsModelString("tableName", "");
        m_onTableExistsAction = new SettingsModelString("existingTable", TableExistsAction.FAIL.getActionCommand());
        m_fileFormat = new SettingsModelString("fileFormat", defaultFormat.toString());
        final Optional<String> compression = getCompressionsForFileformat(m_fileFormat.getStringValue()).stream().findFirst();
        m_compression = new SettingsModelString("compression", compression.orElse("NONE"));
    }

    /**
     * Enum to model what happens when the table already exists.
     */
    public enum TableExistsAction implements ButtonGroupEnumInterface {

        /**
         * Fail if table exists
         */
        FAIL,

        /**
         * Drop table if exists
         */
        DROP;

        @Override
        public String getText() {
            switch (this) {
                case DROP:
                    return "Remove existing table";
                default:
                    return "Fail if table exists";
            }
        }

        @Override
        public String getActionCommand() {
            return this.toString();
        }

        @Override
        public String getToolTip() {
            return null;
        }

        @Override
        public boolean isDefault() {
            return this == FAIL;
        }
    }


    /**
     * @return settings model for the schema name
     */
    public SettingsModelString getSchemaModel() {
        return m_schema;
    }

    /**
     * @return table name
     */
    public String getSchema(){
        return m_schema.getStringValue();
    }

    /**
     * @return settings model for the table name
     */
    public SettingsModelString getTableNameModel() {
        return m_tableName;
    }

    /**
     * @return table name
     */
    public String getTableName(){
        return m_tableName.getStringValue();
    }

    /**
     * @return settings model for whether to do if table exists action
     */
    public SettingsModelString getTableExistsActionModel() {
        return m_onTableExistsAction;
    }

    /**
     * @return whether to do if table already exists action
     */
    public TableExistsAction onExistingTableAction(){
        return TableExistsAction.valueOf(m_onTableExistsAction.getStringValue());
    }

    /**
     * @return settings model for the file format of the table
     * @see FileFormat
     */
    public SettingsModelString getFileFormatModel() {
        return m_fileFormat;
    }

    /**
     * @return the file format of the table
     * @see FileFormat
     */
    public String getFileFormat(){
        return m_fileFormat.getStringValue();
    }

    /**
     * @return settings model for the compression scheme to use
     */
    public SettingsModelString getCompressionModel() {
        return m_compression;
    }

    /**
     * @return the compression scheme to use
     */
    public String getCompression(){
        return m_compression.getStringValue();
    }
    /**
     * Saves  additional Settings to the NodeSettings
     * @param settings the NodeSettingsRO to validate.
     */
    public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_schema.saveSettingsTo(settings);
        m_tableName.saveSettingsTo(settings);
        m_onTableExistsAction.saveSettingsTo(settings);
        m_fileFormat.saveSettingsTo(settings);
        m_compression.saveSettingsTo(settings);
    }

    /**
     * Validates the given Settings
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_schema.validateSettings(settings);
        m_tableName.validateSettings(settings);
        m_onTableExistsAction.validateSettings(settings);
        m_fileFormat.validateSettings(settings);
        m_compression.validateSettings(settings);
    }

    /**
     * Loads the given Settings
     * @param settings the NodeSettingsRO to load.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_schema.loadSettingsFrom(settings);
        m_tableName.loadSettingsFrom(settings);
        m_onTableExistsAction.loadSettingsFrom(settings);
        m_fileFormat.loadSettingsFrom(settings);
        m_compression.loadSettingsFrom(settings);
    }

    /**
     * Returns a list of available compressions for the file format.
     *
     * @param format the file format
     * @return List of compressions for the given fileFormat
     */
    public Collection<String> getCompressionsForFileformat(final String format) {
        final FileFormat fileFormat = FileFormat.fromDialogString(format);
        final List<String> compressions;
        switch (fileFormat) {
            case ORC:
                compressions = OrcCompression.getStringValues();
                break;
            case PARQUET:
                compressions = ParquetCompression.getStringValues();
                break;
            case CLUSTER_DEFAULT:
                compressions = new ArrayList<>();
                compressions.add("<<Cluster Default>>");
                break;
            default:
                compressions = new ArrayList<>();
                compressions.add("NONE");
        }
        return compressions;
    }
}
