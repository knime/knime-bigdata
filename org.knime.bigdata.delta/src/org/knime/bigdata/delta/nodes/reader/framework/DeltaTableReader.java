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
 *   2025-05-21 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.delta.nodes.reader.framework;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;

import org.knime.bigdata.delta.nodes.reader.DeltaTableReaderConfig;
import org.knime.bigdata.delta.types.DeltaTableDataType;
import org.knime.bigdata.delta.types.DeltaTableValue;
import org.knime.bigdata.delta.types.DeltaTableTypeHelper;
import org.knime.bigdata.hadoop.filesystem.NioFileSystemUtil;
import org.knime.core.node.ExecutionMonitor;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.node.table.reader.SourceGroup;
import org.knime.filehandling.core.node.table.reader.TableReader;
import org.knime.filehandling.core.node.table.reader.config.TableReadConfig;
import org.knime.filehandling.core.node.table.reader.read.Read;
import org.knime.filehandling.core.node.table.reader.read.ReadUtils;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec.TypedReaderTableSpecBuilder;

import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

/**
 * Delta table reader implementation.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public final class DeltaTableReader
    implements TableReader<DeltaTableReaderConfig, DeltaTableDataType, DeltaTableValue> {

    private final boolean m_forDialog;

    /**
     * Default constructor.
     *
     * @param forDialog whether this instance is intended for use in the node dialog.
     *      This returns an empty schema if dialog values are unset, the file or a table does not
     *      exist (because old settings remained while switching a file) instead of throwing an exception because
     *      these errors will occur in normal dialog operation.
     */
    public DeltaTableReader(final boolean forDialog) {
        m_forDialog = forDialog;
    }

    @Override
    @SuppressWarnings("resource")
    public Read<DeltaTableValue> read(final FSPath path, final TableReadConfig<DeltaTableReaderConfig> config)
        throws IOException {

        final var hadoopConfig = NioFileSystemUtil.getConfiguration();
        final var hadoopPath = NioFileSystemUtil.getHadoopPath(path, hadoopConfig);
        final var fs = hadoopPath.getFileSystem(hadoopConfig);

        try {
            final var engine = DefaultEngine.create(hadoopConfig);
            final var table = Table.forPath(engine, hadoopPath.toString());
            final var snapshot = table.getLatestSnapshot(engine);
            final var readSchema = filterColumns(snapshot.getSchema(engine));
            final var scan = snapshot.getScanBuilder(engine) //
                     .withReadSchema(engine, readSchema) // column filter
                    // .withFilter(engine, filter) // value filter
                    .build();

            final var scanState = scan.getScanState(engine);
            final var physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);

            final var scanFilesIterator = scan.getScanFiles(engine);
            final var scanFileRowsIterator = new DeltaTableRowIterator(scanFilesIterator);
            final var read = new DeltaTableRead(fs, engine, scanState, scanFileRowsIterator, physicalReadSchema);

            return decorateForReading(read, config);
        } catch (final Exception e) { // NOSONAR
            fs.close(); // ensure we close the hadoop filesystem
            throw e;
        }
    }

    @Override
    public boolean canBeReadInParallel(final SourceGroup<FSPath> sourceGroup) {
        return false;
    }

    @Override
    public TypedReaderTableSpec<DeltaTableDataType> readSpec(final FSPath path,
        final TableReadConfig<DeltaTableReaderConfig> config, final ExecutionMonitor exec) throws IOException {

        // silence path not found exceptions in dialog
        if (m_forDialog && !Files.exists(path)) {
            return new TypedReaderTableSpec<>(Collections.emptyList());
        }

        final var hadoopConfig = NioFileSystemUtil.getConfiguration();
        final var hadoopPath = NioFileSystemUtil.getHadoopPath(path, hadoopConfig);

        try (final var hadoopFileSystem = hadoopPath.getFileSystem(hadoopConfig)) {
            final var tablePath = hadoopPath.toString();
            final var engine = DefaultEngine.create(hadoopConfig);
            final var table = Table.forPath(engine, tablePath);
            final var snapshot = table.getLatestSnapshot(engine);
            final var schema = snapshot.getSchema(engine);

            return readSpec(config, schema);
        } catch (final TableNotFoundException e) {
            if (m_forDialog) { // silence not found exceptions in dialog
                return new TypedReaderTableSpec<>(Collections.emptyList());
            } else {
                throw e;
            }
        }
    }

    private static TypedReaderTableSpec<DeltaTableDataType> readSpec(
        final TableReadConfig<DeltaTableReaderConfig> config, final StructType schema) {

        final TypedReaderTableSpecBuilder<DeltaTableDataType> specBuilder = new TypedReaderTableSpecBuilder<>();
        final var failOnUnsupportedColumnTypes = failOnUnsupportedColumnTypes(config);
        final var fields = schema.fields();

        for (var ordinal = 0; ordinal < fields.size(); ordinal++) {
            DeltaTableTypeHelper.getParquetColumn(fields.get(ordinal), ordinal, failOnUnsupportedColumnTypes)
                .addColumnSpecs(specBuilder);
        }

        return specBuilder.build();
    }

    private static StructType filterColumns(final StructType schema) {
        final var fields = schema.fields();
        final var result = new ArrayList<StructField>(fields.size());

        for (var ordinal = 0; ordinal < fields.size(); ordinal++) {
            final var field = fields.get(ordinal);
            final var col = DeltaTableTypeHelper.getParquetColumn(field, ordinal, false);
            if (!col.skipColumn()) {
                result.add(field);
            }
        }

        return new StructType(result);
    }

    private static boolean failOnUnsupportedColumnTypes(final TableReadConfig<DeltaTableReaderConfig> config) {
        return config.getReaderSpecificConfig().failOnUnsupportedColumnTypes();
    }

    @SuppressWarnings("resource") // closing the read is the responsibility of the caller
    private static Read<DeltaTableValue> decorateForReading(final DeltaTableRead read,
        final TableReadConfig<DeltaTableReaderConfig> config) {
        Read<DeltaTableValue> filtered = read;
        if (config.skipRows()) {
            final var numRowsToSkip = config.getNumRowsToSkip();
            filtered = ReadUtils.skip(filtered, numRowsToSkip);
        }
        if (config.limitRows()) {
            final var numRowsToKeep = config.getMaxRows();
            filtered = ReadUtils.limit(filtered, numRowsToKeep);
        }
        return filtered;
    }

}
