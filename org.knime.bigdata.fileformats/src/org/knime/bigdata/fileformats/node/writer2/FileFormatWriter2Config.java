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
 *   Nov 3, 2020 (dietzc): created
 */
package org.knime.bigdata.fileformats.node.writer2;

import java.util.Arrays;
import java.util.stream.Stream;

import org.knime.bigdata.fileformats.utility.FileFormatFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelLongBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.filehandling.core.defaultnodesettings.EnumConfig;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.filehandling.core.util.SettingsUtils;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;

/*
 * @author Christian Dietz, KNIME GmbH, Konstanz Germany
 */
final class FileFormatWriter2Config<T> {

    /** name of the settings */
    public static final String CFG_TYPE_MAPPING_TAB = "type_mapping";

    /** The settings key for the file chooser dialog */
    private static final String CFG_FILE_CHOOSER = "file_chooser_settings";

    /** The settings for type mapping */
    private static final String CFG_TYPE_MAPPING = "input_type_mapping";

    private static final String CFG_FILE_NAME_PREFIX = "file_name_prefix";

    /** The settings key for compression type */
    private static final String CFG_COMPRESSION = "file_compression";

    /** The settings key for chunk size */
    private static final String CFG_CHUNK_SIZE = "within_file_chunk_size";

    /** The settings key for block size */
    private static final String CFG_FILE_SIZE = "file_size";

    private final SettingsModelWriterFileChooser m_fileChooserModel;

    private final SettingsModelString m_fileNamePrefix;

    private final SettingsModelString m_compression;

    private final SettingsModelIntegerBounded m_chunkSize;

    private SettingsModelLongBounded m_fileSize;

    private final SettingsModelDataTypeMapping<T> m_mappingModel;

    private final FileFormatFactory<T> m_formatFactory;


    public FileFormatWriter2Config(final PortsConfiguration portsConfig, final FileFormatFactory<T> factory) {
        m_formatFactory = factory;

        m_fileChooserModel = new SettingsModelWriterFileChooser(CFG_FILE_CHOOSER, portsConfig,
            AbstractFileFormatWriter2NodeFactory.CONNECTION_INPUT_PORT_GRP_NAME, EnumConfig.create(FilterMode.FILE, FilterMode.FOLDER),
            EnumConfig.create(FileOverwritePolicy.FAIL, m_formatFactory.getSupportedPolicies()),
            // TODO limit to this suffix?
            new String[]{factory.getFilenameSuffix()});

        m_fileNamePrefix = new SettingsModelString(CFG_FILE_NAME_PREFIX, "part_");

        m_compression = new SettingsModelString(CFG_COMPRESSION, "UNCOMPRESSED");

        m_chunkSize =
                new SettingsModelIntegerBounded(CFG_CHUNK_SIZE, factory.getDefaultChunkSize(), 1, Integer.MAX_VALUE);

        m_fileSize = new SettingsModelLongBounded(CFG_FILE_SIZE, factory.getDefaultFileSize(), 1, Long.MAX_VALUE);

        m_mappingModel = factory.getTypeMappingModel(CFG_TYPE_MAPPING, DataTypeMappingDirection.KNIME_TO_EXTERNAL);
    }

    /*
     * LOADING / SAVING / VALIDATING SETTINGS
     */
    final void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettingsTab(settings.getNodeSettings(SettingsUtils.CFG_SETTINGS_TAB));
        loadTypeMappingTab(settings.getNodeSettings(CFG_TYPE_MAPPING_TAB));
    }

    final void saveSettingsTo(final NodeSettingsWO settings) {
        saveSettingsTab(SettingsUtils.getOrAdd(settings, SettingsUtils.CFG_SETTINGS_TAB));
        saveTypeMappingTab(SettingsUtils.getOrAdd(settings, CFG_TYPE_MAPPING_TAB));
    }

    final void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        validateSettingsTab(settings.getNodeSettings(SettingsUtils.CFG_SETTINGS_TAB));
        validateTypeMappingTab(settings.getNodeSettings(CFG_TYPE_MAPPING_TAB));
    }

    private final void loadSettingsTab(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileChooserModel.loadSettingsFrom(settings);
        m_compression.loadSettingsFrom(settings);
        m_fileSize.loadSettingsFrom(settings);
        m_fileNamePrefix.loadSettingsFrom(settings);
        m_chunkSize.loadSettingsFrom(settings);
    }

    private final void loadTypeMappingTab(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_mappingModel.loadSettingsFrom(settings);
    }

    private void saveTypeMappingTab(final NodeSettingsWO settings) {
        m_mappingModel.saveSettingsTo(settings);
    }

    private void saveSettingsTab(final NodeSettingsWO settings) {
        m_fileChooserModel.saveSettingsTo(settings);
        m_compression.saveSettingsTo(settings);
        m_fileSize.saveSettingsTo(settings);
        m_fileNamePrefix.saveSettingsTo(settings);
        m_chunkSize.saveSettingsTo(settings);
    }

    private final void validateTypeMappingTab(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_mappingModel.validateSettings(settings);
    }

    private final void validateSettingsTab(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileChooserModel.validateSettings(settings);
        m_compression.validateSettings(settings);
        m_chunkSize.validateSettings(settings);
        m_fileSize.validateSettings(settings);
        m_fileNamePrefix.validateSettings(settings);
        final int chunkSize = m_chunkSize.<SettingsModelInteger>createCloneWithValidatedValue(settings).getIntValue();
        final SettingsModelLong fileSizeModel = m_fileSize.<SettingsModelLong>createCloneWithValidatedValue(settings);
        final long fileSize = fileSizeModel.getLongValue();
        if (fileSizeModel.isEnabled() && chunkSize > fileSize) {
            throw new InvalidSettingsException("File size must be greater or equals the "
                    + getChunkUnit().toLowerCase() + " size");
        }
    }


    // show flowvariable models which flowvariables can be overriden
    String[] getLocationKeyChain() {
        return Stream
            .concat(Stream.of(SettingsUtils.CFG_SETTINGS_TAB), Arrays.stream(m_fileChooserModel.getKeysForFSLocation()))
            .toArray(String[]::new);
    }

    String[] getPrefixKeyChain() {
        return new String[] {SettingsUtils.CFG_SETTINGS_TAB, m_fileNamePrefix.getKey()};
    }
    SettingsModelWriterFileChooser getFileChooserModel() {
        return m_fileChooserModel;
    }

    /*
     * ACCESS TO MODELS
     */
    SettingsModelString getCompressionModel() {
        return m_compression;
    }

    SettingsModelNumber getChunkSizeModel() {
        return m_chunkSize;
    }

    SettingsModelNumber getFileSizeModel() {
        return m_fileSize;
    }

    SettingsModelDataTypeMapping<T> getMappingModel() {
        return m_mappingModel;
    }

    /*
     * OTHER ACCESSORS
     */
    String[] getCompressionList() {
        return m_formatFactory.getCompressionList();
    }

    String getChunkUnit() {
        return m_formatFactory.getChunkUnit();
    }

    String getChunkSizeUnit() {
        return m_formatFactory.getChunkSizeUnit();
    }

    DataTypeMappingService<T, ?, ?> getTypeMappingService() {
        return m_formatFactory.getTypeMappingService();
    }

    String getSelectedCompression() {
        return m_compression.getStringValue();
    }

    int getChunkSize() {
        return m_chunkSize.getIntValue();
    }

    long getFileSize() {
        return m_fileSize.getLongValue();
    }

    String getFileNamePrefix() {
        return m_fileNamePrefix.getStringValue();
    }

    SettingsModelString getfileNamePrefixModel() {
        return m_fileNamePrefix;
    }
}
