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
 */

package org.knime.bigdata.orc.node.reader;

import java.util.stream.Stream;

import org.apache.orc.CompressionKind;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * <code>OrcReaderNodeSettings</code> for the "OrcReader" Node.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcReaderNodeSettings {
    /**
     * Configuration key for the filename
     */
    public static final String CFGKEY_FILE = "filename";

    private final SettingsModelString m_fileName = new SettingsModelString(CFGKEY_FILE, "");

    private final SettingsModelBoolean m_readRowKey = new SettingsModelBoolean("writerowKey", false);

    private final SettingsModelIntegerBounded m_batchSize = new SettingsModelIntegerBounded("batchsize", 1024, 1,
            Integer.MAX_VALUE);

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
        m_fileName.setStringValue(fileName);
    }

    /**
     * @param writeRowKey whether row key should be written
     */
    void setWriteRowKey(boolean writeRowKey) {
        m_readRowKey.setBooleanValue(writeRowKey);
    }

    /**
     * @return the batch size
     */
    public int getBatchSize() {
        return m_batchSize.getIntValue();
    }

    /**
     * @return whether row key should be written
     */
    boolean getReadRowKey() {
        return m_readRowKey.getBooleanValue();
    }

    /**
     * @return the settings model for the file name
     */
    SettingsModelString getFileNameModel() {
        return m_fileName;
    }

    /**
     * @return the settings model for the row key writing
     */
    SettingsModelBoolean getRowKeyModel() {
        return m_readRowKey;
    }

    /**
     * @return the settings model for the batch size
     */
    SettingsModelInteger getBatchSizeModel() {
        return m_batchSize;
    }

    /**
     * @param settings the NodeSettingsWO to write to.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_fileName.saveSettingsTo(settings);
        m_readRowKey.saveSettingsTo(settings);
        m_batchSize.saveSettingsTo(settings);
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileName.loadSettingsFrom(settings);
        m_readRowKey.loadSettingsFrom(settings);
        m_batchSize.loadSettingsFrom(settings);
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileName.validateSettings(settings);
        m_readRowKey.validateSettings(settings);
        m_batchSize.validateSettings(settings);
    }

    /**
     * @return the list of available compressions
     */
    String[] getCompressionList() {
        return Stream.of(CompressionKind.values()).map(Enum::name).toArray(String[]::new);
    }
}
