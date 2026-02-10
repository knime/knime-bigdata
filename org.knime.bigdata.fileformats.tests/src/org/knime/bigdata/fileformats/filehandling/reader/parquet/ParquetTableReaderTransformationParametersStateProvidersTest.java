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
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.knime.base.node.io.filehandling.webui.reader2.MultiFileReaderParameters.HowToCombineColumnsOption;
import org.knime.base.node.io.filehandling.webui.reader2.ReaderSpecific;
import org.knime.base.node.io.filehandling.webui.reader2.TransformationParameters;
import org.knime.base.node.io.filehandling.webui.testing.reader2.TransformationParametersUpdatesTest;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.PrimitiveKnimeType;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.util.Pair;
import org.knime.filehandling.core.connections.FSLocation;
import org.knime.filehandling.core.node.table.reader.ProductionPathProvider;

/**
 * Test for ParquetTableReader transformation parameters state providers.
 *
 * @author Thomas Reifenberger, TNG Technology Consulting GmbH, Germany
 */
final class ParquetTableReaderTransformationParametersStateProvidersTest
    extends TransformationParametersUpdatesTest<ParquetTableReaderNodeParameters, KnimeType> {

    @Override
    protected void setSourcePath(final ParquetTableReaderNodeParameters settings, final FSLocation fsLocation) {
        settings.m_parquetReaderParameters.m_multiFileSelectionParams.m_source.m_path = fsLocation;
    }

    @Override
    protected void setHowToCombineColumns(final ParquetTableReaderNodeParameters settings,
        final HowToCombineColumnsOption howToCombineColumns) {
        settings.m_parquetReaderParameters.m_multiFileReaderParams.m_howToCombineColumns = howToCombineColumns;
    }

    @Override
    protected TransformationParameters<KnimeType>
        getTransformationSettings(final ParquetTableReaderNodeParameters params) {
        return params.m_transformationParameters;
    }

    @Override
    protected void writeFileWithIntegerAndStringColumn(final String filePath) throws IOException {
        // Define Parquet schema with integer and string columns
        final MessageType schema = Types.buildMessage() //
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("intCol") //
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("stringCol")
            .named("schema");

        // Create parent directories if they don't exist
        final var path = Paths.get(filePath);
        Files.createDirectories(path.getParent());

        // Write the Parquet file using builder pattern
        final var hadoopPath = new Path(filePath);
        final var conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        try (ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(hadoopPath)
            .withConf(conf).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.SNAPPY).build()) {

            final var groupFactory = new SimpleGroupFactory(schema);

            // Write a few test rows
            for (int i = 0; i < 3; i++) {
                final Group group = groupFactory.newGroup();
                group.add("intCol", i);
                group.add("stringCol", "row" + i);
                writer.write(group);
            }
        }
    }

    @Override
    protected ProductionPathProvider<KnimeType> getProductionPathProvider() {
        return ParquetTableReaderSpecific.PRODUCTION_PATH_PROVIDER;
    }

    @Override
    protected Pair<DataType, Collection<IntOrString>> getUnreachableType() {
        return new Pair<>(BooleanCell.TYPE, List.of(IntOrString.INT));
    }

    @Override
    protected KnimeType getIntType() {
        return PrimitiveKnimeType.INTEGER;
    }

    @Override
    protected KnimeType getStringType() {
        return PrimitiveKnimeType.STRING;
    }

    @Override
    protected KnimeType getDoubleType() {
        return PrimitiveKnimeType.DOUBLE;
    }

    @Override
    protected List<String> getPathToTransformationSettings() {
        return List.of("transformationParameters");
    }

    @Override
    protected ParquetTableReaderNodeParameters constructNewSettings() {
        return new ParquetTableReaderNodeParameters();
    }

    @Override
    protected String getFileName() {
        return "test.parquet";
    }

    @Override
    protected ReaderSpecific.ExternalDataTypeSerializer<KnimeType> getExternalDataTypeSerializer() {
        return new ParquetTableReaderTransformationParameters();
    }

}
