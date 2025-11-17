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
package org.knime.bigdata.iceberg.nodes.reader;

import java.util.Optional;

import org.knime.base.node.io.filehandling.webui.ReferenceStateProvider;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderLayout;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderNodeSettings;
import org.knime.bigdata.iceberg.nodes.reader.IcebergReaderNodeSettings.Settings.MakeSourceFolderSelectionModifier;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileReaderWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification.WidgetGroupModifier;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;

/**
 * Settings store managing all configurations required for the delta table reader node.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
public final class IcebergReaderNodeSettings implements NodeParameters {

    @Persist(configKey = "settings")
    Settings m_settings = new Settings();

    @Persist(configKey = "advanced_settings")
    AdvancedSettings m_advancedSettings = new AdvancedSettings();

    @Persistor(IcebergReaderTransformationSettingsPersistor.class)
    IcebergReaderTransformationSettings m_tableSpecConfig = new IcebergReaderTransformationSettings();

    @Modification(MakeSourceFolderSelectionModifier.class)
    static final class Settings extends CommonReaderNodeSettings.BaseSettings {

        // Replace the file selection widget with a folder selection widget
        static final class MakeSourceFolderSelectionModifier implements Modification.Modifier {
            @Override
            public void modify(final WidgetGroupModifier group) {
                final var sourceWidget = group.find(FileSelectionRef.class);
                sourceWidget.removeAnnotation(FileReaderWidget.class);
                sourceWidget.addAnnotation(FileSelectionWidget.class).withValue(SingleFileSelectionMode.FOLDER)
                    .modify();

                final var origDescription = CommonReaderLayout.File.Source.DESCRIPTION;
                final var newDescription = origDescription +
                    """
                    <br/>
                    <b>Note:</b> When selecting a folder, make sure to select the root folder of the Delta Table
                    which contains the <code>_delta_log</code> folder.
                    """;
                sourceWidget.modifyAnnotation(Widget.class).withProperty("description", newDescription).modify();
            }
        }


    }

    static class AdvancedSettings extends CommonReaderNodeSettings.BaseAdvancedSettings {

        @Widget(title = "Skip first data rows",
            description = "Use this option to skip the specified number of data rows.")
        @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
        @Layout(CommonReaderLayout.DataArea.class)
        @ValueReference(SkipFirstDataRowsRef.class)
        @Persist(configKey = "skip_rows")
        Optional<Long> m_skipFirstDataRows = Optional.empty();

        @Widget(title = "Limit number of rows",
            description = "If enabled, only the specified number of data rows are read.")
        @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
        @Layout(CommonReaderLayout.DataArea.class)
        @ValueReference(LimitNumberOfRowsRef.class)
        @Persist(configKey = "limit_rows")
        Optional<Long> m_limitNumberOfRows = Optional.empty();

        public enum OnUnsupportedColumnTypeOption {
                @Label(value = "Fail", //
                    description = "If set, the node fails on Delta Tables with unsupported column types") //
                FAIL, //
                @Label(value = "Ignore columns", //
                    description = "If set, the columns with unsupported column types are ignored.") //
                IGNORE; //
        }

        @Widget(title = "If there are unsupported column types", description = """
            Delta tables can contain columns with types that are not supported by this node,
            for example complex nested types.
            This option allows to select whether the node should fail, or just ignore such columns.""")
        @RadioButtonsWidget
        @Layout(CommonReaderLayout.ColumnAndDataTypeDetection.class)
        @ValueReference(OnUnsupportedColumnTypesRef.class)
        @Persist(configKey = "fail_on_unsupported_column_type")
        OnUnsupportedColumnTypeOption m_onUnsupportedColumnTypes = OnUnsupportedColumnTypeOption.FAIL;

        static final class SkipFirstDataRowsRef extends ReferenceStateProvider<Optional<Long>> {
        }

        static final class LimitNumberOfRowsRef extends ReferenceStateProvider<Optional<Long>> {
        }

        static final class OnUnsupportedColumnTypesRef extends ReferenceStateProvider<OnUnsupportedColumnTypeOption> {
        }

    }

}
