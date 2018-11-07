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
package org.knime.bigdata.fileformats.node.reader;

import org.knime.bigdata.fileformats.utility.FileFormatFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;

/**
 * Settings for generic file format reader.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileFormatReaderNodeSettings {

    /**
     * Configuration key for the filename
     */
    public static final String CFGKEY_FILE = "filename";

    private static final String CFKEY_TYPE_MAPPING = "input_type_mapping";

    private final SettingsModelString m_fileName = new SettingsModelString(CFGKEY_FILE, "");

    private final FileFormatFactory m_formatFactory;

    private final SettingsModelDataTypeMapping<?> m_mappingModel;

    /**
     * Creates initial settings for the given file format.
     *
     * @param formatFactory
     *            the file format
     */
    public FileFormatReaderNodeSettings(final FileFormatFactory formatFactory) {
        m_formatFactory = formatFactory;
        m_mappingModel = m_formatFactory.getTypeMappingModel(CFKEY_TYPE_MAPPING, 
                DataTypeMappingDirection.EXTERNAL_TO_KNIME);
    }

    /**
     * @return the fileName
     */
    String getFileName() {
        return m_fileName.getStringValue();
    }

    /**
     * @return the settings model for the file name
     */
    SettingsModelString getFileNameModel() {
        return m_fileName;
    }

    /**
     * @return the m_formatFactory
     */
    public FileFormatFactory getFormatFactory() {
        return m_formatFactory;
    }

    /**
     * @return the m_mappingModel
     */
    public SettingsModelDataTypeMapping<?> getMappingModel() {
        return m_mappingModel;
    }

    /**
     * @param settings
     *            the NodeSettingsRO to read from.
     * @throws InvalidSettingsException
     *             if the settings are invalid.
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileName.loadSettingsFrom(settings);
        if (settings.containsKey(CFKEY_TYPE_MAPPING)) {
            m_mappingModel.loadSettingsFrom(settings);
        }
    }

    /**
     * @param settings
     *            the NodeSettingsWO to write to.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_fileName.saveSettingsTo(settings);
        m_mappingModel.saveSettingsTo(settings);
    }

    /**
     * @param fileName
     *            the fileName to set
     */
    void setFileName(final String fileName) {
        m_fileName.setStringValue(fileName);
    }

    /**
     * @param settings
     *            the NodeSettingsRO to read from.
     * @throws InvalidSettingsException
     *             if the settings are invalid.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileName.validateSettings(settings);
        if(settings.containsKey(CFKEY_TYPE_MAPPING)) {
            m_mappingModel.validateSettings(settings);
        }
    }

}
