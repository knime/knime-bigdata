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
 *   May 8, 2024 (marcbux): created
 */
package org.knime.bigdata.iceberg.nodes.reader;

import java.util.Optional;

import org.knime.base.node.io.filehandling.webui.reader.CommonReaderTransformationSettings;
import org.knime.bigdata.iceberg.nodes.reader.IcebergReaderNodeSettings.AdvancedSettings.LimitNumberOfRowsRef;
import org.knime.bigdata.iceberg.nodes.reader.IcebergReaderNodeSettings.AdvancedSettings.OnUnsupportedColumnTypeOption;
import org.knime.bigdata.iceberg.nodes.reader.IcebergReaderNodeSettings.AdvancedSettings.OnUnsupportedColumnTypesRef;
import org.knime.bigdata.iceberg.nodes.reader.IcebergReaderNodeSettings.AdvancedSettings.SkipFirstDataRowsRef;
import org.knime.core.webui.node.dialog.defaultdialog.layout.WidgetGroup.Modification;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.ValueProvider;
import org.knime.filehandling.core.node.table.reader.config.DefaultTableReadConfig;

/**
 * Transformation settings of the delta table reader node.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
@Modification(IcebergReaderTransformationSettingsStateProviders.TransformationSettingsWidgetModification.class)
final class IcebergReaderTransformationSettings
    extends CommonReaderTransformationSettings<IcebergReaderTransformationSettings.ConfigIdSettings, String> {

    IcebergReaderTransformationSettings() {
        super(new ConfigIdSettings());
    }

    static final class ConfigIdSettings
        extends CommonReaderTransformationSettings.ConfigIdSettings<IcebergReaderConfig> {

        @ValueProvider(SkipFirstDataRowsRef.class)
        Optional<Long> m_skipFirstDataRows = Optional.empty();

        @ValueProvider(LimitNumberOfRowsRef.class)
        Optional<Long> m_limitNumberOfRows = Optional.empty();

        @ValueProvider(OnUnsupportedColumnTypesRef.class)
        OnUnsupportedColumnTypeOption m_failOnUnsupportedColumnTypes;

        @Override
        protected void applyToConfig(final DefaultTableReadConfig<IcebergReaderConfig> config) {
            config.setColumnHeaderIdx(-1);
            config.setRowIDIdx(-1);

            config.setSkipRows(m_skipFirstDataRows.isPresent());
            config.setNumRowsToSkip(m_skipFirstDataRows.orElse(0L));
            config.setLimitRows(m_limitNumberOfRows.isPresent());
            config.setMaxRows(m_limitNumberOfRows.orElse(0L));

            final var readerConfig = config.getReaderSpecificConfig();
            readerConfig
                .setFailOnUnsupportedColumnTypes(m_failOnUnsupportedColumnTypes == OnUnsupportedColumnTypeOption.FAIL);
        }
    }

}
