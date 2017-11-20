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
 *   Created on 09.05.2014 by thor
 */
package org.knime.bigdata.impala.node.loader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.base.node.io.database.DBSQLTypesPanel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Impala Loader node.
 *
 * @author Thorsten Meinl, KNIME AG, Zurich, Switzerland
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
class ImpalaLoaderSettings {

    /** Target folder settings key */
    public static final String CFG_TARGET_FOLDER = "targetFolder";

    private String m_tableName;

    private String m_targetFolder;

    private NodeSettings m_typeMap = new NodeSettings("typeMap");

    private boolean m_dropTableIfExists;

    private final List<String> m_partitionColumns = new ArrayList<>();

    /**
     * Returns the name of the table in the database.
     *
     * @return a table name
     */
    public String tableName() {
        return m_tableName;
    }

    /**
     * Sets the name of the table in the database.
     *
     * @param name a table name
     */
    public void tableName(final String name) {
        m_tableName = name;
    }

    /**
     * Returns the target folder on the Impala server into which the data file is uploaded.
     *
     * @return a folder path
     */
    public String targetFolder() {
        return m_targetFolder;
    }

    /**
     * Sets the target folder on the Impala server into which the data file is uploaded.
     *
     * @param folder a folder path
     */
    public void targetFolder(final String folder) {
        m_targetFolder = folder;
    }

    /**
     * Returns whether an existing table in the database should be dropped first.
     *
     * @return <code>true</code> if the table should be dropped, <code>false</code> if data should be appended
     */
    public boolean dropTableIfExists() {
        return m_dropTableIfExists;
    }

    /**
     * Sets whether an existing table in the database should be dropped first.
     *
     * @param b <code>true</code> if the table should be dropped, <code>false</code> if data should be appended
     */
    public void dropTableIfExists(final boolean b) {
        m_dropTableIfExists = b;
    }

    /**
     * Clears the KNIME-to-Database type mapping.
     */
    public void clearTypeMapping() {
        m_typeMap = new NodeSettings("typeMap");
    }

    /**
     * Returns the SQL data type for the given column.
     *
     * @param column a column name
     * @return the corresponding SQL data type
     */
    public String typeMapping(final String column) {
        return m_typeMap.getString(column, null);
    }

    /**
     * Sets the SQL data type for the given column.
     *
     * @param column a column name
     * @param sqlType the SQL data type for the column
     */
    public void typeMapping(final String column, final String sqlType) {
        m_typeMap.addString(column, sqlType);
    }

    /**
     * Returns the type mapping as a node settings object. The strcuture matches the one expected by the
     * {@link DBSQLTypesPanel}.
     *
     * @return the type mapping
     */
    public NodeSettings getTypeMapping() {
        return m_typeMap;
    }

    /**
     * Returns the columns that should be used for partitioning.
     *
     * @return a collection of columns names
     */
    public Collection<String> partitionColumns() {
        return Collections.unmodifiableCollection(m_partitionColumns);
    }

    /**
     * Sets the columns that should be used for partitioning.
     *
     * @param partColumns a collection of columns names
     */
    public void partitionColumn(final Collection<String> partColumns) {
        m_partitionColumns.clear();
        m_partitionColumns.addAll(partColumns);
    }

    /**
     * Loads the settings from the given settings object.
     *
     * @param settings a node settings object
     * @throws InvalidSettingsException if settings are invalid or missing
     */
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_tableName = settings.getString("tableName");
        m_targetFolder = settings.getString(CFG_TARGET_FOLDER);
        m_dropTableIfExists = settings.getBoolean("dropTableIfExists");
        clearTypeMapping();
        settings.getNodeSettings("typeMap").copyTo(m_typeMap);

        m_partitionColumns.clear();
        String[] partColumns = settings.getStringArray("partitionColumns");
        for (String s : partColumns) {
            m_partitionColumns.add(s);
        }
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     *
     * @param settings a node settings object
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        m_tableName = settings.getString("tableName", "");
        m_targetFolder = settings.getString(CFG_TARGET_FOLDER, "");
        m_dropTableIfExists = settings.getBoolean("dropTableIfExists", false);
        clearTypeMapping();
        try {
            settings.getNodeSettings("typeMap").copyTo(m_typeMap);
        } catch (InvalidSettingsException ex) {
            // ignore it
        }

        m_partitionColumns.clear();
        String[] partColumns = settings.getStringArray("partitionColumns", new String[0]);
        for (String s : partColumns) {
            m_partitionColumns.add(s);
        }
    }

    /**
     * Saves the settings into the given settings object.
     *
     * @param settings a node settings object
     */
    public void saveSettings(final NodeSettingsWO settings) {
        settings.addString("tableName", m_tableName);
        settings.addString(CFG_TARGET_FOLDER, m_targetFolder);
        settings.addBoolean("dropTableIfExists", m_dropTableIfExists);
        settings.addStringArray("partitionColumns", m_partitionColumns.toArray(new String[m_partitionColumns.size()]));
        settings.addNodeSettings(m_typeMap);
    }

    /**
     * Guesses a type mapping for the incoming data table. Existing type mappings are retained only new mappings are
     * added.
     *
     * @param inputSpec the input table's spec
     * @param ignoreEmptyMapping <code>true</code> if empty mappings should be ignored (only useful for the dialog),
     *            <code>false</code> if they should be rejected with an {@link InvalidSettingsException}
     * @throws InvalidSettingsException if an illegal type mapping for a column was found
     */
    public void guessTypeMapping(final DataTableSpec inputSpec, final boolean ignoreEmptyMapping)
        throws InvalidSettingsException {
        Map<String, String> typeMap = new HashMap<>();

        for (DataColumnSpec colSpec : inputSpec) {
            final String name = colSpec.getName();
            String sqlType = typeMapping(name);
            if (sqlType == null) {
                final DataType type = colSpec.getType();
                if (type.isCompatible(IntValue.class)) {
                    sqlType = "INT";
                } else if (type.isCompatible(DoubleValue.class)) {
                    sqlType = "DOUBLE";
                } else if (type.isCompatible(DateAndTimeValue.class)) {
                    sqlType = "TIMESTAMP";
                } else {
                    sqlType = "STRING";
                }
            } else if (sqlType.trim().isEmpty() && !ignoreEmptyMapping) {
                throw new InvalidSettingsException("No type mapping for column '" + name + "' defined");
            }
            typeMap.put(name, sqlType);
        }
        clearTypeMapping();
        for (Map.Entry<String, String> e : typeMap.entrySet()) {
            typeMapping(e.getKey(), e.getValue());
        }
    }
}
