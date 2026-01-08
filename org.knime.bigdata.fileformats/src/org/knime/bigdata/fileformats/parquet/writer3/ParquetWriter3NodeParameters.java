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
 * ------------------------------------------------------------------------
 */

package org.knime.bigdata.fileformats.parquet.writer3;

import java.util.List;
import java.util.function.Supplier;

import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.widget.PersistWithin;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.legacy.LegacyFileWriterWithOverwritePolicyOptions;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.EnumChoicesProvider;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;
import org.knime.node.parameters.widget.text.TextInputWidget;

/**
 * Node parameters for Parquet Writer.
 *
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 * @author AI Migration Pipeline v1.2
 */
@LoadDefaultsForAbsentFields
@SuppressWarnings("restriction")
class ParquetWriter3NodeParameters implements NodeParameters {

    /**
     * Settings section containing output location and storage configuration
     */
    @Section(title = "Settings", description = "General settings regarding the output file location and storage configuration.")
    interface SettingsSection {
    }

    @Persist(configKey = "file_chooser_settings")
    @PersistWithin("settings")
    @Modification(OutputFileModification.class)
    @Layout(SettingsSection.class)
    LegacyFileWriterWithOverwritePolicyOptions m_outputLocation = new LegacyFileWriterWithOverwritePolicyOptions();

    @Widget(title = "File mode", description = "false = file mode, true = folder mode")
    @ValueSwitchWidget
    @Layout(SettingsSection.class)
    @Persist(configKey = "filter_mode")
    @PersistWithin({"settings", "file_chooser_settings", "filter_mode"})
    @ValueReference(FilterModeReference.class)
    SingleFileSelectionMode m_singleFileSelectionMode = SingleFileSelectionMode.FILE;

    @Widget(title = "File Compression",
            description = "The compression codec used to write the Parquet file.")
    @org.knime.node.parameters.persistence.Persist(configKey = "file_compression")
    @PersistWithin("settings")
    @ChoicesProvider(CompressionChoicesProvider.class)
    @Layout(SettingsSection.class)
    String m_compression = "UNCOMPRESSED";

    @Widget(title = "Split data into files of size (MB)",
            description = "Splits up the input data into files of the specified maximum size in megabytes. "
                + "This option is only available if the folder mode is selected.")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @org.knime.node.parameters.persistence.Persist(configKey = "file_size")
    @PersistWithin("settings")
    @Layout(SettingsSection.class)
    long m_fileSize = 1024;

    @Widget(title = "File name prefix",
            description = "The prefix to use for the file within the selected folder. A running index is appended "
                + "starting with 0 e.g. part_00000.parquet, part_00001.parquet. This option is only available if "
                + "the folder mode is selected.")
    @TextInputWidget
    @org.knime.node.parameters.persistence.Persist(configKey = "file_name_prefix")
    @PersistWithin("settings")
    @Layout(SettingsSection.class)
    String m_fileNamePrefix = "part_";

    @Widget(title = "Within file row group size (MB)",
            description = "Defines the maximum size of a row group within a file in megabyte. For more details see "
                + "the <a href=\"https://parquet.apache.org/docs/\">Parquet documentation</a>.")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @org.knime.node.parameters.persistence.Persist(configKey = "within_file_chunk_size")
    @PersistWithin("settings")
    @Layout(SettingsSection.class)
    int m_chunkSize = 128; // Default from ParquetWriter.DEFAULT_BLOCK_SIZE / TO_BYTE

    static final class FilterModeReference implements ParameterReference<SingleFileSelectionMode> {}

    /**
     * Choices provider for compression codecs
     */
    static class CompressionChoicesProvider implements StringChoicesProvider {
        @Override
        public List<String> choices(final NodeParametersInput context) {
            return List.of(
                "UNCOMPRESSED",
                "SNAPPY",
                "GZIP",
                "ZSTD"
            );
        }
    }

    /**
     * Modification to customize the file writer widget
     */
    static final class OutputFileModification implements LegacyFileWriterWithOverwritePolicyOptions.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            final var fileSelection = findFileSelection(group);

            // Remove FileWriterWidget and add FileSelectionWidget to support both file and folder modes
            fileSelection
                .removeAnnotation(org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileWriterWidget.class);
            fileSelection
                .addAnnotation(FileSelectionWidget.class)
                .withProperty("singleFileSelectionModeProvider", ParquetWriter3FileSelectionModeProvider.class)
                .modify();

            fileSelection
                .modifyAnnotation(Widget.class)
                .withProperty("title", "Output location")
                .withProperty("description",
                    "Select a file system and location where you want to store the file. "
                    + "<br/><b>Mode:</b> Choose between writing to a single file or splitting the data into multiple files in a folder. "
                    + "When writing to a folder, the data is split into files of the specified size with the configured prefix.")
                .modify();

            final var overwritePolicy = findOverwritePolicy(group);
            overwritePolicy
                .addAnnotation(ChoicesProvider.class)
                .withProperty("value", OverwritePolicyChoicesProvider.class)
                .modify();
        }
    }

    private static final class ParquetWriter3FileSelectionModeProvider
        implements StateProvider<SingleFileSelectionMode>{

        private Supplier<SingleFileSelectionMode> m_modeSupplier;

        /**
         * {@inheritDoc}
         */
        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeBeforeOpenDialog();
            m_modeSupplier = initializer.computeFromValueSupplier(FilterModeReference.class);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SingleFileSelectionMode computeState(final NodeParametersInput parametersInput)
            throws StateComputationFailureException {
            return m_modeSupplier.get();
        }
    }

    /**
     * Choices provider for overwrite policy that only allows FAIL and OVERWRITE
     */
    private static final class OverwritePolicyChoicesProvider
        implements EnumChoicesProvider<LegacyFileWriterWithOverwritePolicyOptions.OverwritePolicy> {

        @Override
        public List<LegacyFileWriterWithOverwritePolicyOptions.OverwritePolicy>
            choices(final NodeParametersInput context) {
            return List.of(
                LegacyFileWriterWithOverwritePolicyOptions.OverwritePolicy.fail,
                LegacyFileWriterWithOverwritePolicyOptions.OverwritePolicy.overwrite
            );
        }
    }

    @Section(title = "Output Type Mapping", sideDrawer = true)
    @Advanced
    @After(SettingsSection.class)
    interface OutputMappingSection {
    }

    @Layout(OutputMappingSection.class)
    DBOutputTypeMappingParameters m_outputMapping = new DBOutputTypeMappingParameters();
}
