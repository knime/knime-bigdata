/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   2025-05-21 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.delta.nodes.reader;

import org.knime.bigdata.delta.nodes.reader.mapper.DeltaTableReadAdapterFactory;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings;
import org.knime.filehandling.core.node.table.ConfigSerializer;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigID;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigIDFactory;
import org.knime.filehandling.core.node.table.reader.config.tablespec.DefaultProductionPathSerializer;
import org.knime.filehandling.core.node.table.reader.config.tablespec.NodeSettingsConfigID;
import org.knime.filehandling.core.node.table.reader.config.tablespec.NodeSettingsSerializer;
import org.knime.filehandling.core.node.table.reader.config.tablespec.TableSpecConfigSerializer;

/**
 * {@link ConfigSerializer} for Delta Table Reader node.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
enum DeltaTableReaderConfigSerializer implements ConfigSerializer<DeltaTableReaderMultiTableReadConfig>,
        ConfigIDFactory<DeltaTableReaderMultiTableReadConfig> {

    /**
     * Singleton instance.
     */
    INSTANCE;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DeltaTableReaderConfigSerializer.class);

    private static final String CFG_SETTINGS_ID = "delta_table_reader";

    private static final String CFG_TABLE_SPEC_CONFIG = "table_spec_config" + SettingsModel.CFGKEY_INTERNAL;

    private final TableSpecConfigSerializer<DataType> m_tableSpecSerializer;

    public enum DataTypeSerializer implements NodeSettingsSerializer<DataType> {

        SERIALIZER_INSTANCE;

        private static final String CFG_TYPE = "type";

        @Override
        public void save(final DataType object, final NodeSettingsWO settings) {
            settings.addDataType(CFG_TYPE, object);
        }

        @Override
        public DataType load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return settings.getDataType(CFG_TYPE);
        }

    }

    DeltaTableReaderConfigSerializer() {
        m_tableSpecSerializer = TableSpecConfigSerializer.createStartingV44(
                new DefaultProductionPathSerializer(DeltaTableReadAdapterFactory.INSTANCE.getProducerRegistry()),
                this, DataTypeSerializer.SERIALIZER_INSTANCE);
    }

    @Override
    public ConfigID createFromConfig(final DeltaTableReaderMultiTableReadConfig config) {
        final var settings = new NodeSettings(CFG_SETTINGS_ID);

        return new NodeSettingsConfigID(settings);
    }

    @Override
    public ConfigID createFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        return new NodeSettingsConfigID(settings.getNodeSettings(CFG_SETTINGS_ID));
    }

    @Override
    public void loadInDialog(final DeltaTableReaderMultiTableReadConfig config, final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            final var specific = config.getReaderSpecificConfig();
            final var read = DefaultNodeSettings.loadSettings(settings, DeltaTableReaderNodeSettings.class);
            specific.loadValidatedSettingsFrom(read);
            config.loadValidatedSettingsFrom(specific);
        } catch (InvalidSettingsException e) { // NOSONAR use defaults
        }
        if (config.saveTableSpecConfig() && settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            try {
                config.setTableSpecConfig(m_tableSpecSerializer.load(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG)));
            } catch (InvalidSettingsException ex) { // NOSONAR, see below
                /*
                 * Can only happen in TableSpecConfig#load, since we checked
                 * #NodeSettingsRO#getNodeSettings(String) before. The framework takes care that
                 * #validate is called before load so we can assume that this exception does not
                 * occur.
                 */
            }
        } else {
            config.setTableSpecConfig(null);
        }
    }

    @Override
    public void loadInModel(final DeltaTableReaderMultiTableReadConfig config, final NodeSettingsRO settings)
            throws InvalidSettingsException {
        final var specific = config.getReaderSpecificConfig();
        final var read = DefaultNodeSettings.loadSettings(settings, DeltaTableReaderNodeSettings.class);
        specific.loadValidatedSettingsFrom(read);
        config.loadValidatedSettingsFrom(specific);
        if (config.saveTableSpecConfig() && settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            config.setTableSpecConfig(m_tableSpecSerializer.load(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG)));
        } else {
            config.setTableSpecConfig(null);
        }
    }


    @Override
    public void saveInModel(final DeltaTableReaderMultiTableReadConfig config, final NodeSettingsWO settings) {
        DefaultNodeSettings.saveSettings(DeltaTableReaderNodeSettings.class, config.getReaderSpecificConfig(),
            settings);
        if (config.saveTableSpecConfig() && config.hasTableSpecConfig()) {
            m_tableSpecSerializer.save(config.getTableSpecConfig(), settings.addNodeSettings(CFG_TABLE_SPEC_CONFIG));
        }
    }

    @Override
    public void saveInDialog(final DeltaTableReaderMultiTableReadConfig config, final NodeSettingsWO settings)
            throws InvalidSettingsException {
        saveInModel(config, settings);
    }

    @Override
    public void validate(final DeltaTableReaderMultiTableReadConfig config, final NodeSettingsRO settings)
            throws InvalidSettingsException {
        DefaultNodeSettings.loadSettings(settings, DeltaTableReaderNodeSettings.class);
        if (settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            m_tableSpecSerializer.load(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG));
        }
    }

    TableSpecConfigSerializer<DataType> getTableSpecSerializer() {
        return m_tableSpecSerializer;
    }
}
