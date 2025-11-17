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
package org.knime.bigdata.iceberg.nodes.reader;

import org.knime.bigdata.iceberg.nodes.reader.mapper.IcebergReadAdapterFactory;
import org.knime.bigdata.iceberg.types.IcebergDataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersUtil;
import org.knime.filehandling.core.node.table.ConfigSerializer;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigID;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigIDFactory;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigIDLoader;
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
enum IcebergReaderConfigSerializer implements ConfigSerializer<IcebergReaderMultiTableReadConfig>,
        ConfigIDFactory<IcebergReaderMultiTableReadConfig> {

    /**
     * Singleton instance.
     */
    INSTANCE;

    private static final String CFG_SETTINGS_ID = "delta_table_reader";

    private static final String CFG_TABLE_SPEC_CONFIG = "table_spec_config" + SettingsModel.CFGKEY_INTERNAL;

    private final TableSpecConfigSerializer<IcebergDataType> m_tableSpecSerializer;

    enum DeltaTableDataTypeSerializer implements NodeSettingsSerializer<IcebergDataType> {

        SERIALIZER_INSTANCE;

        private static final String CFG_TYPE = "type";

        @Override
        public void save(final IcebergDataType externalType, final NodeSettingsWO settings) {
            settings.addString(CFG_TYPE, externalType.toSerializableType());
        }

        @Override
        public IcebergDataType load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return IcebergDataType.toExternalType(settings.getString(CFG_TYPE));
        }

    }

    IcebergReaderConfigSerializer() {
        m_tableSpecSerializer = getTableSpecSerializer(this);
    }

    @Override
    public ConfigID createFromConfig(final IcebergReaderMultiTableReadConfig config) {
        final var settings = new NodeSettings(CFG_SETTINGS_ID);

        return new NodeSettingsConfigID(settings);
    }

    @Override
    public ConfigID createFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        return new NodeSettingsConfigID(settings.getNodeSettings(CFG_SETTINGS_ID));
    }

    @Override
    public void loadInDialog(final IcebergReaderMultiTableReadConfig config, final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {

        throw new NotConfigurableException("Not implemented, using WebUI Dialog instead");
    }

    @Override
    public void loadInModel(final IcebergReaderMultiTableReadConfig config, final NodeSettingsRO settings)
            throws InvalidSettingsException {

        final var read = NodeParametersUtil.loadSettings(settings, IcebergReaderNodeSettings.class);
        config.loadValidatedSettingsFrom(read);
        if (config.saveTableSpecConfig() && settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            config.setTableSpecConfig(m_tableSpecSerializer.load(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG)));
        } else {
            config.setTableSpecConfig(null);
        }
    }

    @Override
    public void saveInModel(final IcebergReaderMultiTableReadConfig config, final NodeSettingsWO settings) {
        final var defaultSettings = new IcebergReaderNodeSettings();
        NodeParametersUtil.saveSettings(IcebergReaderNodeSettings.class, defaultSettings, settings);
        if (config.saveTableSpecConfig() && config.hasTableSpecConfig()) {
            m_tableSpecSerializer.save(config.getTableSpecConfig(), settings.addNodeSettings(CFG_TABLE_SPEC_CONFIG));
        }
    }

    @Override
    public void saveInDialog(final IcebergReaderMultiTableReadConfig config, final NodeSettingsWO settings)
            throws InvalidSettingsException {
        throw new InvalidSettingsException("Not implemented, using WebUI Dialog instead");
    }

    @Override
    public void validate(final IcebergReaderMultiTableReadConfig config, final NodeSettingsRO settings)
            throws InvalidSettingsException {
        NodeParametersUtil.loadSettings(settings, IcebergReaderNodeSettings.class);
        if (settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            m_tableSpecSerializer.load(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG));
        }
    }

    static TableSpecConfigSerializer<IcebergDataType> getTableSpecSerializer(final ConfigIDLoader configIdLoader) {
        return TableSpecConfigSerializer.createStartingV44(
            new DefaultProductionPathSerializer(IcebergReadAdapterFactory.INSTANCE.getProducerRegistry()),
            configIdLoader, DeltaTableDataTypeSerializer.SERIALIZER_INSTANCE);
    }

}
