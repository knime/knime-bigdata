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
 *   Nov 6, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.orc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataReaderConfig;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataValueAccessFactory;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.ListKnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.PrimitiveKnimeType;
import org.knime.bigdata.hadoop.filesystem.NioFileSystemUtil;
import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.node.ExecutionMonitor;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.node.table.reader.GenericTableReader;
import org.knime.filehandling.core.node.table.reader.TableReader;
import org.knime.filehandling.core.node.table.reader.config.TableReadConfig;
import org.knime.filehandling.core.node.table.reader.ftrf.adapter.SequentialBatchReadableAdapter;
import org.knime.filehandling.core.node.table.reader.read.Read;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec.TypedReaderTableSpecBuilder;

/**
 * {@link TableReader} for reading from ORC files.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class OrcTableReader implements GenericTableReader<FSPath, BigDataReaderConfig, KnimeType, BigDataCell> {

    @Override
    public Read<FSPath, BigDataCell> read(final FSPath path, final TableReadConfig<BigDataReaderConfig> config)
        throws IOException {
        Reader reader = createReader(path);
        return new OrcRead(reader, path);
    }

    private static Reader createReader(final FSPath path) throws IOException {
        Configuration hadoopConfig = NioFileSystemUtil.getConfiguration();
        org.apache.hadoop.fs.Path hadoopPath = NioFileSystemUtil.getHadoopPath(path, hadoopConfig);
        return OrcFile.createReader(hadoopPath, OrcFile.readerOptions(hadoopConfig));
    }

    @Override
    public TypedReaderTableSpec<KnimeType> readSpec(final FSPath path, final TableReadConfig<BigDataReaderConfig> config,
        final ExecutionMonitor exec) throws IOException {
        final Reader reader = createReader(path);
        return createSpec(reader.getSchema());
    }

    static TypedReaderTableSpec<KnimeType> createSpec(final TypeDescription schema) {
        final TypedReaderTableSpecBuilder<KnimeType> builder = new TypedReaderTableSpecBuilder<>();
        final Iterator<TypeDescription> childIterator = schema.getChildren().iterator();
        final Iterator<String> nameIterator = schema.getFieldNames().iterator();
        while (childIterator.hasNext()) {
            assert nameIterator.hasNext() : "At least one column has no name.";
            builder.addColumn(nameIterator.next(), toKnimeType(childIterator.next()), true);
        }
        return builder.build();
    }

    // Come on sonar, it doesn't get much simpler
    private static KnimeType toKnimeType(final TypeDescription type) {//NOSONAR
        final Category category = type.getCategory();
        switch (category) {
            case BOOLEAN:
                return PrimitiveKnimeType.BOOLEAN;
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return PrimitiveKnimeType.DOUBLE;
            case BYTE:
            case INT:
            case SHORT:
                return PrimitiveKnimeType.INTEGER;
            case LONG:
                return PrimitiveKnimeType.LONG;
            case CHAR:
            case VARCHAR:
            case STRING:
                return PrimitiveKnimeType.STRING;
            case LIST:
                return new ListKnimeType(toKnimeType(type.getChildren().get(0)));
            case DATE:
                return PrimitiveKnimeType.DATE;
            case TIMESTAMP:
                return PrimitiveKnimeType.LOCAL_DATE_TIME;
            case BINARY:
                return PrimitiveKnimeType.BINARY;
            // unsupported types (they are listed intentionally so that we can see what is supported with one glance)
            case MAP:
            case STRUCT:
            case UNION:
            default:
                throw new IllegalArgumentException(String.format("The ORC type '%s' is not supported.", category));
        }
    }

    @Override
    public SequentialBatchReadable readContent(final FSPath item, final TableReadConfig<BigDataReaderConfig> config,
        final TypedReaderTableSpec<KnimeType> spec) {
        return new SequentialBatchReadableAdapter<>(item, config, spec, this, 1024, BigDataValueAccessFactory.INSTANCE);
    }

}
