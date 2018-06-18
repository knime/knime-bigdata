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

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;

/**
 * Interface for reader factories.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public interface FileFormatFactory {

    /**
     * Creates a file reader for the given file.
     *
     * @param file the file or directory to read from
     * @param isReadRowKey if the row key has to be read
     * @param batchSize the batch size for reading
     * @param exec the execution context
     * @return the reader
     */
    public AbstractFileFormatReader getReader(final RemoteFile<Connection> file, final boolean isReadRowKey,
            final int batchSize, final ExecutionContext exec);

    /**
     * Returns a writer that writes KNIME {@link DataRow}s to an speicifc file
     * format
     *
     * @param file the target file
     * @param spec the data table spec
     * @param isWriteRowKey whether the key should be written
     * @param batchSize size of the batch
     * @param compression the compression to use for writing
     * @return the writer
     * @throws IOException throw if writer cannot be created
     */
    public AbstractFileFormatWriter getWriter(final RemoteFile<Connection> file, final DataTableSpec spec,
            final boolean isWriteRowKey, final int batchSize, final String compression) throws IOException;

    /**
     * Returns a list of Strings containing all compressionCodecs supported by
     * the specified file format.
     *
     * @return the list of available compressions
     */
    public String[] getCompressionList();

    /**
     * Returns an array of all unsupported types for a given table spec, array
     * might be empty if all types are supported.
     *
     * @param spec the table spec to check
     * @return the array containing the unsupported types
     */
    public abstract String[] getUnsupportedTypes(final DataTableSpec spec);

    /**
     * @return non-blank short name shown in dialogs
     */
    String getName();

    /** @return file name suffix for files, e.g. 'bin' or 'orc'. */
    String getFilenameSuffix();

}
