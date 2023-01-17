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
package org.knime.bigdata.fileformats.orc;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.TypeDescription;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.node.writer2.FileFormatWriter;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCTypeMappingService;
import org.knime.bigdata.fileformats.orc.datatype.mapping.SettingsModelORCDataTypeMapping;
import org.knime.bigdata.fileformats.orc.reader.OrcKNIMEReader;
import org.knime.bigdata.fileformats.orc.writer.OrcKNIMEWriter;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.bigdata.fileformats.utility.FileFormatFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;

/**
 * Factory for ORC format
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcFormatFactory implements FileFormatFactory<TypeDescription> {

    private static final String STRIPE = "Stripe";

    private static final String SUFFIX = ".orc";

    private static final String NAME = "ORC";

    private static final String[] COMPRESSION_LIST = new String[]{ //
        CompressionKind.NONE.name(), //
        CompressionKind.ZLIB.name(), //
        CompressionKind.SNAPPY.name(), //
        CompressionKind.LZO.name(), //
        CompressionKind.LZ4.name() //
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public String getChunkSizeUnit() {
        return "rows";
    }

    @Override
    public String getChunkUnit() {
        return STRIPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDefaultFileSize() {
        return 1000000;
    }

    @Override
    public int getDefaultChunkSize() {
        return VectorizedRowBatch.DEFAULT_SIZE;
    }

    @Override
    public String[] getCompressionList() {
        return COMPRESSION_LIST;
    }

    @Override
    public String getFilenameSuffix() {
        return SUFFIX;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AbstractFileFormatReader getReader(final RemoteFile<Connection> file, final ExecutionContext exec,
        final DataTypeMappingConfiguration<TypeDescription> outputDataTypeMappingConfiguration,
        final boolean useKerberos) {
        try {
            return new OrcKNIMEReader(file, outputDataTypeMappingConfiguration, exec, useKerberos);
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
    }

    @Override
    public SettingsModelDataTypeMapping<TypeDescription> getTypeMappingModel(final String key,
        final DataTypeMappingDirection mappingDirection) {
        return new SettingsModelORCDataTypeMapping(key, mappingDirection);
    }

    @Override
    public ORCTypeMappingService getTypeMappingService() {
        return ORCTypeMappingService.getInstance();
    }

    @Deprecated
    @Override
    public AbstractFileFormatWriter getWriter(final RemoteFile<Connection> file, final DataTableSpec spec,
        final int chunkSize, final String compression,
        final DataTypeMappingConfiguration<TypeDescription> typeMappingConf) throws IOException {
        return new OrcKNIMEWriter(file, spec, chunkSize, compression, typeMappingConf);
    }

    @Override
    public FileFormatWriter getWriter(final FSPath path, final FileOverwritePolicy overwritePolicy,
        final DataTableSpec spec, final long fileSize, final int chunkSize,
        final String compression, final DataTypeMappingConfiguration<TypeDescription> typeMappingConf) throws IOException {
    	//overwritePolicy can be ignored since ORC only supports creation of new files
        return new OrcFileFormatWriter(path, spec, fileSize,
            chunkSize, CompressionKind.valueOf(compression), typeMappingConf);
    }
}
