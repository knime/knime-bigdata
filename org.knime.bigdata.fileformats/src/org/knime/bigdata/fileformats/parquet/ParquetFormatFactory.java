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
package org.knime.bigdata.fileformats.parquet;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.parquet.reader.ParquetKNIMEReader;
import org.knime.bigdata.fileformats.parquet.writer.ParquetKNIMEWriter;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.bigdata.fileformats.utility.FileFormatFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;

/**
 * Factory for Parquet file format.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ParquetFormatFactory implements FileFormatFactory {

    private static final String SUFFIX = ".parquet";
    private static final String NAME = "Parquet";
    private static final int TO_BYTE = 1024 * 1024;

    @Override
    public AbstractFileFormatReader getReader(final RemoteFile<Connection> file, final ExecutionContext exec) {
        try {
            return new ParquetKNIMEReader(file, exec);
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
    }

    @Override
    public AbstractFileFormatWriter getWriter(final RemoteFile<Connection> file, final DataTableSpec spec,
            final int chunkSize, final String compression) throws IOException {
        return new ParquetKNIMEWriter(file, spec, compression, chunkSize * TO_BYTE);
    }

    @Override
    public String[] getCompressionList() {
        return Stream.of(CompressionCodecName.values()).filter(i -> i != CompressionCodecName.LZO).map(Enum::name)
                .toArray(String[]::new);
    }

    @Override
    public String[] getUnsupportedTypes(final DataTableSpec spec) {
        return ParquetTableStoreFormat.getUnsupportedTypes(spec);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getFilenameSuffix() {
        return SUFFIX;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getChunkSizeUnit() {
        return "MB";
    }
}
