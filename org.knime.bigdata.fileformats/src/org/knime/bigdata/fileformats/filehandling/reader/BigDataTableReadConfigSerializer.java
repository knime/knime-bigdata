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
 *   Sep 30, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader;

import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.ListKnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.PrimitiveKnimeType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.node.table.reader.config.ConfigSerializer;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigID;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigIDFactory;
import org.knime.filehandling.core.node.table.reader.config.tablespec.DefaultProductionPathSerializer;
import org.knime.filehandling.core.node.table.reader.config.tablespec.NodeSettingsConfigID;
import org.knime.filehandling.core.node.table.reader.config.tablespec.NodeSettingsSerializer;
import org.knime.filehandling.core.node.table.reader.config.tablespec.TableSpecConfig;
import org.knime.filehandling.core.node.table.reader.config.tablespec.TableSpecConfigSerializer;
import org.knime.filehandling.core.util.SettingsUtils;

/**
 * {@link ConfigSerializer} for big data file format readers.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
enum BigDataTableReadConfigSerializer
    implements ConfigSerializer<BigDataMultiTableReadConfig>, ConfigIDFactory<BigDataMultiTableReadConfig> {
        INSTANCE;

    private static final String CFG_FAIL_ON_DIFFERING_SPECS = "fail_on_differing_specs";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BigDataTableReadConfigSerializer.class);

    private static final String CFG_TABLE_SPEC_CONFIG = "table_spec_config" + SettingsModel.CFGKEY_INTERNAL;

    private static final String CFG_SAVE_TABLE_SPEC_CONFIG = "save_table_spec_config" + SettingsModel.CFGKEY_INTERNAL;

    private final TableSpecConfigSerializer<KnimeType> m_tableSpecConfigSerializer;

    private enum KnimeTypeSerializer implements NodeSettingsSerializer<KnimeType> {
            SERIALIZER;

        private static final String CFG_NESTING_LEVEL = "is_list";

        private static final String CFG_PRIMITIVE = "primitive";

        @Override
        public void save(final KnimeType object, final NodeSettingsWO settings) {
            int depth = 0;
            KnimeType elementType = object;
            for (; elementType.isList(); depth++) {
                elementType = elementType.asListType().getElementType();
            }
            settings.addInt(CFG_NESTING_LEVEL, depth);
            settings.addString(CFG_PRIMITIVE, elementType.asPrimitiveType().name());
        }

        @Override
        public KnimeType load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final int nestingLevel = settings.getInt(CFG_NESTING_LEVEL);
            final PrimitiveKnimeType primitive = PrimitiveKnimeType.valueOf(settings.getString(CFG_PRIMITIVE));
            KnimeType knimeType = primitive;
            for (int i = nestingLevel; i > 0; i--) {
                knimeType = new ListKnimeType(knimeType);
            }
            return knimeType;
        }

    }

    private BigDataTableReadConfigSerializer() {
        m_tableSpecConfigSerializer = TableSpecConfigSerializer.createStartingV43(
            new DefaultProductionPathSerializer(BigDataReadAdapterFactory.INSTANCE.getProducerRegistry()), this,
            KnimeTypeSerializer.SERIALIZER);
    }

    @Override
    public void loadInDialog(final BigDataMultiTableReadConfig config, final NodeSettingsRO settings,
        final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            config.setTableSpecConfig(loadTableSpecConfig(settings));
        } catch (InvalidSettingsException ex) {
            LOGGER.debug("Failed to load the TableSpecConfig", ex);
        }
        final NodeSettingsRO settingsTab = SettingsUtils.getOrEmpty(settings, SettingsUtils.CFG_SETTINGS_TAB);
        config.setFailOnDifferingSpecs(settingsTab.getBoolean(CFG_FAIL_ON_DIFFERING_SPECS, true));
        config.setSaveTableSpecConfig(settingsTab.getBoolean(CFG_SAVE_TABLE_SPEC_CONFIG, true));
    }

    private TableSpecConfig<KnimeType> loadTableSpecConfig(final NodeSettingsRO settings)
        throws InvalidSettingsException {

        if (settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            return m_tableSpecConfigSerializer.load(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG));
        } else {
            return null;
        }
    }

    @Override
    public void loadInModel(final BigDataMultiTableReadConfig config, final NodeSettingsRO settings)
        throws InvalidSettingsException {
        config.setTableSpecConfig(loadTableSpecConfig(settings));
        final NodeSettingsRO settingsTab = settings.getNodeSettings(SettingsUtils.CFG_SETTINGS_TAB);
        config.setFailOnDifferingSpecs(settingsTab.getBoolean(CFG_FAIL_ON_DIFFERING_SPECS));
        // introduced with 4.3.1
        if (settingsTab.containsKey(CFG_SAVE_TABLE_SPEC_CONFIG)) {
            config.setSaveTableSpecConfig(settingsTab.getBoolean(CFG_SAVE_TABLE_SPEC_CONFIG));
        }
    }

    @Override
    public void saveInModel(final BigDataMultiTableReadConfig config, final NodeSettingsWO settings) {
        if (config.hasTableSpecConfig()) {
            m_tableSpecConfigSerializer.save(config.getTableSpecConfig(),
                settings.addNodeSettings(CFG_TABLE_SPEC_CONFIG));
        }
        final NodeSettingsWO settingsTab = SettingsUtils.getOrAdd(settings, SettingsUtils.CFG_SETTINGS_TAB);
        settingsTab.addBoolean(CFG_FAIL_ON_DIFFERING_SPECS, config.failOnDifferingSpecs());
        settingsTab.addBoolean(CFG_SAVE_TABLE_SPEC_CONFIG, config.saveTableSpecConfig());
    }

    @Override
    public void saveInDialog(final BigDataMultiTableReadConfig config, final NodeSettingsWO settings)
        throws InvalidSettingsException {
        saveInModel(config, settings);
    }

    @Override
    public void validate(final BigDataMultiTableReadConfig config, final NodeSettingsRO settings)
        throws InvalidSettingsException {
        if (settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            m_tableSpecConfigSerializer.load(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG));
        }
        final NodeSettingsRO settingsTab = settings.getNodeSettings(SettingsUtils.CFG_SETTINGS_TAB);
        settingsTab.getBoolean(CFG_FAIL_ON_DIFFERING_SPECS);
        // added in 4.3.1
        if (settingsTab.containsKey(CFG_SAVE_TABLE_SPEC_CONFIG)) {
            settingsTab.getBoolean(CFG_SAVE_TABLE_SPEC_CONFIG);
        }
    }

    @Override
    public ConfigID createFromConfig(final BigDataMultiTableReadConfig config) {
        return new NodeSettingsConfigID(new NodeSettings("big_data_reader"));
    }

    @Override
    public ConfigID createFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        return new NodeSettingsConfigID(settings.getNodeSettings("big_data_reader"));
    }

}
