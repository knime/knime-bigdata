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
 *  This program is distributed in the hope that it will be useful, but.
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

import org.knime.base.node.io.filehandling.webui.FileSystemPortConnectionUtil;
import org.knime.base.node.io.filehandling.webui.ReferenceStateProvider;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderLayout.ColumnAndDataTypeDetection;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderLayout.DataArea.LimitNumberOfRows;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderLayout.DataArea.MaximumNumberOfRows;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderLayout.DataArea.SkipFirstDataRows;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderNodeSettings;
import org.knime.bigdata.delta.nodes.reader.DeltaTableReaderNodeLayout.AdvancedDataAreaLayout;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Layout;
import org.knime.core.webui.node.dialog.defaultdialog.persistence.api.Persist;
import org.knime.core.webui.node.dialog.defaultdialog.persistence.api.Persistor;
import org.knime.core.webui.node.dialog.defaultdialog.setting.fileselection.FileSelection;
import org.knime.core.webui.node.dialog.defaultdialog.widget.NumberInputWidget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Widget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.Effect;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.Effect.EffectType;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.Predicate;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.PredicateProvider;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.ValueReference;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;
import org.knime.filehandling.core.node.table.reader.config.ReaderSpecificConfig;

/**
 * Settings store managing all configurations required for the delta table reader node.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
public final class DeltaTableReaderNodeSettings
    implements DefaultNodeSettings, ReaderSpecificConfig<DeltaTableReaderNodeSettings> {

    @SuppressWarnings("javadoc")
    @Persist(configKey = "settings")
    public Settings m_settings = new Settings();

    @Persist(configKey = "advanced_settings")
    AdvancedSettings m_advancedSettings = new AdvancedSettings();

    @Persistor(DeltaTableReaderTransformationSettingsPersistor.class)
    DeltaTableReaderTransformationSettings m_tableSpecConfig = new DeltaTableReaderTransformationSettings();

    @SuppressWarnings("javadoc")
    public static class Settings extends CommonReaderNodeSettings.BaseSettings {

        /**
         * Constant signal to indicate whether the user has added a (possibly empty) File System port or not.
         */
        static final class HasFileSystemPort implements PredicateProvider {
            @Override
            public Predicate init(final PredicateInitializer i) {
                return i.getConstant(FileSystemPortConnectionUtil::hasFileSystemPortWithConnection);
            }
        }

        void loadValidatedSettingsFrom(final Settings loaded) {
            m_source = new FileSelection(loaded.m_source.getFSLocation());
            m_fileSelectionInternal.m_enabledStatus = loaded.m_fileSelectionInternal.m_enabledStatus;
            m_fileSelectionInternal.m_settingsModelID = loaded.m_fileSelectionInternal.m_settingsModelID;
        }

    }

    static class AdvancedSettings extends CommonReaderNodeSettings.BaseAdvancedSettings {

        @Widget(title = "Skip first data rows", description = SkipFirstDataRows.DESCRIPTION)
        @NumberInputWidget(validation = IsNonNegativeValidation.class)
        @Layout(AdvancedDataAreaLayout.class)
        @ValueReference(SkipFirstDataRowsRef.class)
        @Persistor(CommonReaderNodeSettings.SkipFirstDataRowsPersistor.class)
        long m_skipFirstDataRows;

        @Widget(title = "Limit number of rows", description = LimitNumberOfRows.DESCRIPTION)
        @Layout(AdvancedDataAreaLayout.class)
        @ValueReference(CommonReaderNodeSettings.LimitNumberOfRowsRef.class)
        @Persist(configKey = "limit_data_rows")
        boolean m_limitNumberOfRows;
        // TODO NOSONAR merge into a single widget with UIEXT-1742

        @Widget(title = "Maximum number of rows", description = MaximumNumberOfRows.DESCRIPTION)
        @NumberInputWidget(validation = IsNonNegativeValidation.class)
        @Layout(AdvancedDataAreaLayout.class)
        @ValueReference(MaximumNumberOfRowsRef.class)
        @Effect(predicate = CommonReaderNodeSettings.LimitNumberOfRowsPredicate.class, type = EffectType.SHOW)
        @Persist(configKey = "max_rows")
        long m_maximumNumberOfRows = 50;

        @Widget(title = "Fail on unsupported column types",
            description = "Parquet files can contain columns with types that are not supported by this node,"
                + " for example complex nested types."
                + " This option allows to select whether the node should fail, or just ignore such columns.")
        @Layout(ColumnAndDataTypeDetection.class)
        @ValueReference(FailOnUnsupportedColumnTypesRef.class)
        // TODO: do we need internal here??? See BigDataTableReadConfigSerializer
        @Persist(configKey = "fail_on_unsupported_column_type" + SettingsModel.CFGKEY_INTERNAL)
        boolean m_failOnUnsupportedColumnTypes = true;

        void loadValidatedSettingsFrom(final AdvancedSettings loaded) {
            m_ifSchemaChangesOption = loaded.m_ifSchemaChangesOption;
            m_limitNumberOfRows = loaded.m_limitNumberOfRows;
            m_maximumNumberOfRows = loaded.m_maximumNumberOfRows;
            m_skipFirstDataRows = loaded.m_skipFirstDataRows;
            m_failOnUnsupportedColumnTypes = loaded.m_failOnUnsupportedColumnTypes;
        }

        static final class SkipFirstDataRowsRef extends ReferenceStateProvider<Long> {
        }

        static final class MaximumNumberOfRowsRef extends ReferenceStateProvider<Long> {
        }

        static final class FailOnUnsupportedColumnTypesRef extends ReferenceStateProvider<Boolean> {
        }

    }

    /**
     * Set whenever the reader node should fail on unsupported column types.
     *
     * @param fail {@code true} if the node should fail with an exception on unsupported column types or {@code false}
     *            if columns should be ignored
     */
    public void setFailOnUnsupportedColumnTypes(final boolean fail) {
        m_advancedSettings.m_failOnUnsupportedColumnTypes = fail;
    }

    /**
     * @return {@code true} if the node should fail with an exception on unsupported column types or {@code false} if
     *         columns should be ignored
     */
    public boolean failOnUnsupportedColumnTypes() {
        return m_advancedSettings.m_failOnUnsupportedColumnTypes;
    }

    @Override
    public DeltaTableReaderNodeSettings copy() {
        final var result = new DeltaTableReaderNodeSettings();
        result.loadValidatedSettingsFrom(this);
        return result;
    }

    /**
     * @param loaded the settings to copy over
     */
    void loadValidatedSettingsFrom(final DeltaTableReaderNodeSettings loaded) {
        m_settings.loadValidatedSettingsFrom(loaded.m_settings);
        m_advancedSettings.loadValidatedSettingsFrom(loaded.m_advancedSettings);
    }

}
