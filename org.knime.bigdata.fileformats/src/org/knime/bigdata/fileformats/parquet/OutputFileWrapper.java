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
 *   Sep 16, 2024 (bjoern): created
 */
package org.knime.bigdata.fileformats.parquet;

import java.io.IOException;
import java.nio.file.InvalidPathException;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * Wrapper around {@link OutputFile} which returns null for {@link #getPath()}. This is necessary to work around a bug
 * in Parquet on Windows, where an {@link InvalidPathException} is thrown.
 *
 * Parquet versions higher than 1.14.2 may contain a fix for this.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class OutputFileWrapper implements OutputFile {

    private final OutputFile m_wrapped;

    public OutputFileWrapper(final OutputFile wrapped) {
        m_wrapped = wrapped;
    }

    @Override
    public PositionOutputStream create(final long blockSizeHint) throws IOException {
        return m_wrapped.create(blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(final long blockSizeHint) throws IOException {
        return m_wrapped.createOrOverwrite(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return m_wrapped.supportsBlockSize();
    }

    @Override
    public long defaultBlockSize() {
        return m_wrapped.defaultBlockSize();
    }

    // don't implement the getPath() method on purpose as this triggers an issue in the ParquetWriter constructor
    // because of https://github.com/apache/parquet-java/commit/7be98076f30faad3626155666a8e2a9ebe922c6a
    //
    // The issue occurs on Windows, where the ParquetWriter() constructor tries to parse the output of getPath()
    // using the native NIO file system provider (Paths.get()). The output of getPath() is typically a URL, such
    // as nio-wrapper-1249898://...
    // This fails on Windows with an InvalidPathException because of the ':' character.
}