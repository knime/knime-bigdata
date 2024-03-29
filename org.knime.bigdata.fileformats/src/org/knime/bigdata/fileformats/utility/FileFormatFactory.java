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
 * History 04.06.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.utility;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.node.writer2.FileFormatWriter;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.database.util.DataRows;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;

/**
 * Interface for reader factories.
 *
 * @param <X> the type whose instances describe the external data types
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public interface FileFormatFactory<X> {

    /**
     * @return String containing the chunksizeUnit
     */
    String getChunkSizeUnit();

    /**
     * @return the unit of a chunk
     */
    String getChunkUnit();

    /**
     * @return the default file size to use
     */
    long getDefaultFileSize();

    /**
     * @return the default chunk size to use
     */
    int getDefaultChunkSize();

    /**
     * Returns a list of Strings containing all compressionCodecs supported by the specified file format.
     *
     * @return the list of available compressions
     */
    String[] getCompressionList();

    /** @return file name suffix for files, e.g. 'bin' or 'orc'. */
    String getFilenameSuffix();

    /**
     * @return the supported {@link FileOverwritePolicy}s
     */
    default Set<FileOverwritePolicy> getSupportedPolicies() {
        return EnumSet.of(FileOverwritePolicy.FAIL);
    }

    /**
     * @return non-blank short name shown in dialogs
     */
    String getName();

    /**
     * Creates a file reader for the given file.
     *
     * @param file the file or directory to read from
     * @param exec the execution context
     * @param outputDataTypeMappingConfiguration the type mapping configuration
     * @param useKerberos
     * @return the reader
     */
    AbstractFileFormatReader getReader(final RemoteFile<Connection> file, final ExecutionContext exec,
        DataTypeMappingConfiguration<X> outputDataTypeMappingConfiguration, boolean useKerberos);

    /**
     * Returns the type mapping settings model for the file format
     *
     * @param cfkeyTypeMapping the key to use for the type mapping configuration
     * @param mappingDirection the direction to map
     * @return the settings model
     */
    SettingsModelDataTypeMapping<X> getTypeMappingModel(String cfkeyTypeMapping,
        DataTypeMappingDirection mappingDirection);

    /**
     * @return the {@link DataTypeMappingService} for this file format.
     */
    DataTypeMappingService<X, ?, ?> getTypeMappingService();

    /**
     * Returns a writer that writes KNIME {@link DataRow}s to an specific file format
     *
     * @param file the target file
     * @param spec the data table spec
     * @param chunkSize size of the batch
     * @param compression the compression to use for writing
     * @param typeMappingConf the type mapping configuration
     * @return the writer
     * @throws IOException throw if writer cannot be created
     */
    @Deprecated
    AbstractFileFormatWriter getWriter(final RemoteFile<Connection> file, final DataTableSpec spec, final int chunkSize,
        final String compression, DataTypeMappingConfiguration<X> typeMappingConf) throws IOException;

    /**
     * Returns a writer that writes {@link DataRows} to a specific file format to the provided {@link FSPath}
     *
     * @param path the {@link FSPath} to write to
     * @param overwritePolicy {@link FileOverwritePolicy}
     * @param spec the actual {@link DataTableSpec}
     * @param fileSize total size of a file when writing into a directory
     * @param chunkSize size of a chunk. unit defined by individual formats.
     * @param compression compression type
     * @param typeMappingConf configuration of the type mapping
     * @return the file format writer
     * @throws IOException
     */
    FileFormatWriter getWriter(FSPath path, FileOverwritePolicy overwritePolicy, DataTableSpec spec, long fileSize,
        int chunkSize, String compression, DataTypeMappingConfiguration<X> typeMappingConf) throws IOException;

}
