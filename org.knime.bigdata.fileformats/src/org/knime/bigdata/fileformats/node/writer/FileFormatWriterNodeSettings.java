/*
 * ------------------------------------------------------------------------
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * -------------------------------------------------------------------
 *
 * History 28.05.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.node.writer;

import org.knime.bigdata.fileformats.utility.FileFormatFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for generic BigData file format writer.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileFormatWriterNodeSettings {

    /**
     * Configuration key for the filename
     */
    public static final String CFGKEY_FILE = "filename";

    private final SettingsModelBoolean m_fileOverwritePolicy = new SettingsModelBoolean("overwrite", false);

    private final SettingsModelString m_fileName = new SettingsModelString(CFGKEY_FILE, "");

    private final SettingsModelString m_compression = new SettingsModelString("compression", "UNCOMPRESSED");

    private final SettingsModelIntegerBounded m_chunkSize = new SettingsModelIntegerBounded("chunksize", 100, 1,
            Integer.MAX_VALUE); // default 512MB, min 20MB

    private final SettingsModelIntegerBounded m_numOflocalChunks = new SettingsModelIntegerBounded("numChunks", 4, 1,
            20);

    private final FileFormatFactory m_formatFactory;

    /**
     * Constructor for FileFormatWriterNodeSettings with a specific Format.,
     *
     * @param factory the factory for the file format
     */
    public FileFormatWriterNodeSettings(final FileFormatFactory factory) {
        m_formatFactory = factory;
    }

    /**
     * @return the m_formatFactory
     */
    public FileFormatFactory getFormatFactory() {
        return m_formatFactory;
    }

    /**
     * @param fileOverwritePolicy the fileOverwritePolicy to set
     */
    void setFileOverwritePolicy(final boolean overwrite) {
        m_fileOverwritePolicy.setBooleanValue(overwrite);
    }

    /**
     * @return the fileOverwritePolicy true for overwrite
     */
    boolean getFileOverwritePolicy() {
        return m_fileOverwritePolicy.getBooleanValue();
    }

    /**
     * @return the fileName
     */
    String getFileName() {
        return m_fileName.getStringValue();
    }

    /**
     * @param fileName the fileName to set
     */
    void setFileName(final String fileName) {
        String fileWithSuffix = fileName;
        if (!fileName.isEmpty() && !fileName.endsWith(m_formatFactory.getFilenameSuffix()) && !fileName.endsWith("/")) {
            fileWithSuffix = fileName + m_formatFactory.getFilenameSuffix();
        }
        m_fileName.setStringValue(fileWithSuffix);
    }

    /**
     * @param codec the compression codec name to use
     */
    void setCompression(final String codec) {
        m_compression.setStringValue(codec);
    }

    /**
     * @return the compression codec name
     */
    String getCompression() {
        return m_compression.getStringValue();
    }

    /**
     * @param size the size of a chunk
     */
    void setChunkSize(final int size) {
        m_chunkSize.setIntValue(size);
    }

    /**
     * @return the chunk size
     */
    int getChunkSize() {
        return m_chunkSize.getIntValue();
    }

    /**
     * @param num the number of chunks to keep locally
     */
    void setNumOfLocalChunks(final int num) {
        m_numOflocalChunks.setIntValue(num);
    }

    /**
     * @return the number of local chunks
     */
    int getNumOfLocalChunks() {
        return m_numOflocalChunks.getIntValue();
    }

    /**
     * @return the settings model for the overwrite policy
     */
    SettingsModelBoolean getfileOverwritePolicyModel() {
        return m_fileOverwritePolicy;
    }

    /**
     * @return the settings model for the file name
     */
    SettingsModelString getfileNameModel() {
        return m_fileName;
    }

    /**
     * @return the settings model for the file compression
     */
    SettingsModelString getCompressionModel() {
        return m_compression;
    }

    /**
     * @return the settings model for the chunk size
     */
    SettingsModelNumber getChunkSizeModel() {
        return m_chunkSize;
    }

    /**
     * @return the settings model for the number of local chunks
     */
    SettingsModelNumber getNumOfLocalChunksModel() {
        return m_numOflocalChunks;
    }

    /**
     * Saves the settings to the NodeSettings
     *
     * @param settings the NodeSettingsWO to write to.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_fileOverwritePolicy.saveSettingsTo(settings);
        m_fileName.saveSettingsTo(settings);
        m_compression.saveSettingsTo(settings);
        m_chunkSize.saveSettingsTo(settings);
        m_numOflocalChunks.saveSettingsTo(settings);
    }

    /**
     * Loads the settings from a given NodeSetting
     *
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileOverwritePolicy.loadSettingsFrom(settings);
        m_fileName.loadSettingsFrom(settings);
        m_compression.loadSettingsFrom(settings);
        m_chunkSize.loadSettingsFrom(settings);
        m_numOflocalChunks.loadSettingsFrom(settings);
    }

    /**
     * Validates the given settings
     *
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileOverwritePolicy.validateSettings(settings);
        m_fileName.validateSettings(settings);
        m_compression.validateSettings(settings);
        m_chunkSize.validateSettings(settings);
        m_numOflocalChunks.validateSettings(settings);
    }

    /**
     * Returns a list of Strings containing all compressionCodecs supported by
     * the specified file format.
     *
     * @return the list of available compressions
     */
    String[] getCompressionList() {
        return getFormatFactory().getCompressionList();
    }

    /**
     * @return String with unit for chunksize
     */
    public String getChunksizeUnit() {
        return m_formatFactory.getChunkSizeUnit();
    }
}
