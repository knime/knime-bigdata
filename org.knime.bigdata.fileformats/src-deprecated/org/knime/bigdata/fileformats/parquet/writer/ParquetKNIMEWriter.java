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
package org.knime.bigdata.fileformats.parquet.writer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.Type;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.parquet.OutputFileWrapper;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetParameter;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsWO;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;

/**
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ParquetKNIMEWriter extends AbstractFileFormatWriter {

    /**
     * A class for building instances of {@link ParquetWriter} that are able to
     * write instances of {@link DataRow} to a Parquet file.
     */
    static final class DataRowParquetWriterBuilder extends ParquetWriter.Builder<DataRow, DataRowParquetWriterBuilder> {
        private final WriteSupport<DataRow> m_writeSupport;

        DataRowParquetWriterBuilder(final OutputFile outputFile, final WriteSupport<DataRow> writeSupport) {
            super(outputFile);
            m_writeSupport = writeSupport;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected WriteSupport<DataRow> getWriteSupport(final Configuration conf) {
            return m_writeSupport;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected DataRowParquetWriterBuilder self() {
            return this;
        }
    }

    private final ParquetWriter<DataRow> m_writer;

    DataTypeMappingConfiguration<Type> m_mappingConfig;

    private final ParquetParameter[] m_params;

    /**
     * Constructs a writer for writing a KNIME table to a binary file on the
     * local disk using an instance of {@link ParquetWriter}.
     *
     * @param file the local file to which to write
     * @param spec the specification of the KNIME table to write to disk
     * @param compression The compression codec to use for writing
     * @param rowGroupSize Parquet horizontally divides tables into row groups.
     *        A row group is a logical horizontal partitioning of the data into
     *        rows. This parameter determines the size of a row group (in
     *        Bytes). Row groups are flushed to the file when the current size
     *        of the buffer (held in memory) approaches the specified size.
     * @param inputputDataTypeMappingConfiguration the type mapping configuration
     * @throws IOException an exception that is thrown if something goes wrong
     *         during the initialization of the {@link ParquetWriter}
     */
    @SuppressWarnings("unchecked")
    public ParquetKNIMEWriter(final RemoteFile<Connection> file, final DataTableSpec spec,
            final String compression, final int rowGroupSize,
            final DataTypeMappingConfiguration<?> inputputDataTypeMappingConfiguration)
                    throws IOException {
        super(file, rowGroupSize, spec);
        final CompressionCodecName codec = CompressionCodecName.fromConf(compression);
        m_mappingConfig = (DataTypeMappingConfiguration<Type>) inputputDataTypeMappingConfiguration;
        m_params = new ParquetParameter[spec.getNumColumns()];
        for (int i = 0; i < spec.getNumColumns(); i++) {
            m_params[i] = new ParquetParameter(spec.getColumnSpec(i).getName(), i);
        }


        try {
            final boolean useLogicalTypes = false;

            final OutputFile outputFile = HadoopOutputFile.fromPath(//
                new Path(file.getURI()),//
                ConfigurationFactory.createBaseConfiguration());
            final OutputFile wrappedFile = new OutputFileWrapper(outputFile);

            m_writer = new DataRowParquetWriterBuilder(wrappedFile,
                    new DataRowWriteSupport(spec.getName(), spec,
                            m_mappingConfig.getConsumptionPathsFor(spec), m_params, useLogicalTypes))
                    .withCompressionCodec(codec)
                    .withDictionaryEncoding(true)
                    .withRowGroupSize(rowGroupSize).withWriteMode(Mode.OVERWRITE).build();
        } catch (final InvalidSettingsException e) {
            throw new BigDataFileFormatException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        m_writer.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeMetaInfoAfterWrite(final NodeSettingsWO settings) {
        // no metadata to write
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeRow(final DataRow row) throws IOException {
        m_writer.write(row);
    }
}
