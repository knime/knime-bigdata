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

import org.knime.bigdata.fileformats.filehandling.reader.type.PrimitiveKnimeType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.node.table.reader.config.ConfigSerializer;
import org.knime.filehandling.core.node.table.reader.config.DefaultMultiTableReadConfig;
import org.knime.filehandling.core.node.table.reader.config.DefaultTableReadConfig;
import org.knime.filehandling.core.node.table.reader.config.DefaultTableSpecConfig;
import org.knime.filehandling.core.node.table.reader.config.TableSpecConfig;
import org.knime.filehandling.core.util.SettingsUtils;

/**
 * {@link ConfigSerializer} for big data file format readers.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
enum BigDataTableReadConfigSerializer implements
    ConfigSerializer<DefaultMultiTableReadConfig<BigDataReaderConfig, DefaultTableReadConfig<BigDataReaderConfig>>> {
        INSTANCE;

    private static final String CFG_FAIL_ON_DIFFERING_SPECS = "FAIL_ON_DIFFERING_SPECS";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BigDataTableReadConfigSerializer.class);

    private static final String CFG_TABLE_SPEC_CONFIG = "table_spec_config" + SettingsModel.CFGKEY_INTERNAL;

    @Override
    public void loadInDialog(
        final DefaultMultiTableReadConfig<BigDataReaderConfig, DefaultTableReadConfig<BigDataReaderConfig>> config,
        final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        if (settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            try {
                config.setTableSpecConfig(loadTableSpecConfig(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG)));
            } catch (InvalidSettingsException ex) {
                LOGGER.debug("Failed to load the TableSpecConfig", ex);
            }
        }
        final NodeSettingsRO settingsTab = SettingsUtils.getOrEmpty(settings, SettingsUtils.CFG_SETTINGS_TAB);
        config.setFailOnDifferingSpecs(settingsTab.getBoolean(CFG_FAIL_ON_DIFFERING_SPECS, true));
    }

    private static TableSpecConfig loadTableSpecConfig(final NodeSettingsRO settings) throws InvalidSettingsException {
        return DefaultTableSpecConfig.load(settings, BigDataReadAdapterFactory.INSTANCE.getProducerRegistry(),
            PrimitiveKnimeType.STRING, null);
    }

    @Override
    public void loadInModel(
        final DefaultMultiTableReadConfig<BigDataReaderConfig, DefaultTableReadConfig<BigDataReaderConfig>> config,
        final NodeSettingsRO settings) throws InvalidSettingsException {
        if (settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            config.setTableSpecConfig(loadTableSpecConfig(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG)));
        }
        final NodeSettingsRO settingsTab = settings.getNodeSettings(SettingsUtils.CFG_SETTINGS_TAB);
        config.setFailOnDifferingSpecs(settingsTab.getBoolean(CFG_FAIL_ON_DIFFERING_SPECS));
    }

    @Override
    public void saveInModel(
        final DefaultMultiTableReadConfig<BigDataReaderConfig, DefaultTableReadConfig<BigDataReaderConfig>> config,
        final NodeSettingsWO settings) {
        if (config.hasTableSpecConfig()) {
            config.getTableSpecConfig().save(settings.addNodeSettings(CFG_TABLE_SPEC_CONFIG));
        }
        final NodeSettingsWO settingsTab = SettingsUtils.getOrAdd(settings, SettingsUtils.CFG_SETTINGS_TAB);
        settingsTab.addBoolean(CFG_FAIL_ON_DIFFERING_SPECS, config.failOnDifferingSpecs());
    }

    @Override
    public void saveInDialog(
        final DefaultMultiTableReadConfig<BigDataReaderConfig, DefaultTableReadConfig<BigDataReaderConfig>> config,
        final NodeSettingsWO settings) throws InvalidSettingsException {
        saveInModel(config, settings);
    }

    @Override
    public void validate(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (settings.containsKey(CFG_TABLE_SPEC_CONFIG)) {
            DefaultTableSpecConfig.validate(settings.getNodeSettings(CFG_TABLE_SPEC_CONFIG),
                BigDataReadAdapterFactory.INSTANCE.getProducerRegistry());
        }
        final NodeSettingsRO settingsTab = settings.getNodeSettings(SettingsUtils.CFG_SETTINGS_TAB);
        settingsTab.getBoolean(CFG_FAIL_ON_DIFFERING_SPECS);
    }

}
