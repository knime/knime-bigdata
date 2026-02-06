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
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet;

import static org.knime.node.impl.description.PortDescription.dynamicPort;
import static org.knime.node.impl.description.PortDescription.fixedPort;

import java.util.List;

import org.knime.base.node.io.filehandling.webui.reader2.MultiFileSelectionPath;
import org.knime.base.node.io.filehandling.webui.reader2.NodeParametersConfigAndSourceSerializer;
import org.knime.base.node.io.filehandling.webui.reader2.WebUITableReaderNodeFactory;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataMultiTableReadConfig;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataReadAdapterFactory;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataReaderConfig;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeTypeHierarchies;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.util.Version;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.node.table.reader.GenericTableReader;
import org.knime.filehandling.core.node.table.reader.ReadAdapterFactory;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigID;
import org.knime.filehandling.core.node.table.reader.config.tablespec.ConfigIDLoader;
import org.knime.filehandling.core.node.table.reader.config.tablespec.NodeSettingsConfigID;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeHierarchy;
import org.knime.node.impl.description.DefaultNodeDescriptionUtil;

/**
 * Factory for the Parquet Table Reader Node.
 *
 * @author Robin Gerling, KNIME GmbH, Konstanz, Germany
 */
public final class ParquetTableReader3NodeFactory extends //
    WebUITableReaderNodeFactory<ParquetTableReader3NodeParameters, MultiFileSelectionPath, //
            BigDataReaderConfig, KnimeType, BigDataCell, BigDataMultiTableReadConfig> {

    @SuppressWarnings("javadoc")
    public ParquetTableReader3NodeFactory() {
        super(ParquetTableReader3NodeParameters.class);
    }

    @Override
    protected NodeDescription createNodeDescription() {
        return DefaultNodeDescriptionUtil.createNodeDescription( //
            "Parquet Reader (Labs)", //
            "./parquetreader-icon.png", //
            List.of(dynamicPort(FS_CONNECT_GRP_ID, "File System Connection", "The file system connection.")), //
            List.of(fixedPort("Data Table", "The data table containing the data of the Parquet file.")), //
            "Reader for Parquet files.", //
            "Reader for Parquet files. It reads either single files or all files in a given directory.", //
            List.of(), //
            ParquetTableReader3NodeParameters.class, //
            null, //
            NodeType.Source, //
            List.of("Input", "Read"), //
            new Version(5, 10, 0) //
        );
    }

    @Override
    protected ReadAdapterFactory<KnimeType, BigDataCell> getReadAdapterFactory() {
        return BigDataReadAdapterFactory.INSTANCE;
    }

    @Override
    protected GenericTableReader<FSPath, BigDataReaderConfig, KnimeType, BigDataCell> createReader() {
        return new ParquetTableReader2();
    }

    @Override
    protected String extractRowKey(final BigDataCell value) {
        return value.getString();
    }

    @Override
    protected TypeHierarchy<KnimeType, KnimeType> getTypeHierarchy() {
        return KnimeTypeHierarchies.TYPE_HIERARCHY;
    }

    @Override
    protected ParquetConfigAndSourceSerializer createSerializer() {
        return new ParquetConfigAndSourceSerializer();
    }

    private final class ParquetConfigAndSourceSerializer
        extends NodeParametersConfigAndSourceSerializer<ParquetTableReader3NodeParameters, MultiFileSelectionPath, //
                BigDataReaderConfig, KnimeType, BigDataMultiTableReadConfig> {
        protected ParquetConfigAndSourceSerializer() {
            super(ParquetTableReader3NodeParameters.class);
        }

        @Override
        protected void saveToSourceAndConfig(final ParquetTableReader3NodeParameters params,
            final String existingSourceId, final ConfigID existingConfigId, final MultiFileSelectionPath source,
            final BigDataMultiTableReadConfig config) {
            params.saveToSource(source);
            params.saveToConfig(config, existingSourceId, existingConfigId);
        }

        @Override
        protected ConfigIDLoader getConfigIDLoader() {
            // TODO: Return configIDLoader from BigDataTableReadConfigSerializer when available after moving this factory back to the original package.
            return settings -> new NodeSettingsConfigID(
                settings.getNodeSettings(new ParquetTableReader3TransformationParameters().getConfigIdSettingsKey()));
        }
    }

    @Override
    protected MultiFileSelectionPath createPathSettings(final NodeCreationConfiguration nodeCreationConfig) {
        final var source = new MultiFileSelectionPath();
        final var defaultParams = new ParquetTableReader3NodeParameters();
        defaultParams.saveToSource(source);
        return source;
    }

    @Override
    protected BigDataMultiTableReadConfig createConfig(final NodeCreationConfiguration nodeCreationConfig) {
        final var cfg = new BigDataMultiTableReadConfig();
        final var defaultParams = new ParquetTableReader3NodeParameters();
        defaultParams.saveToConfig(cfg);
        return cfg;
    }
}
