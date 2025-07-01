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
package org.knime.bigdata.iceberg.nodes.reader;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;

import org.apache.xmlbeans.XmlException;
import org.knime.bigdata.iceberg.nodes.reader.framework.IcebergReader;
import org.knime.bigdata.iceberg.nodes.reader.mapper.IcebergReadAdapterFactory;
import org.knime.bigdata.iceberg.types.IcebergDataType;
import org.knime.bigdata.iceberg.types.IcebergValue;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.context.url.URLConfiguration;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationUtil;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.EnumConfig;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.SettingsModelReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.filehandling.core.node.table.reader.AbstractTableReaderNodeFactory;
import org.knime.filehandling.core.node.table.reader.MultiTableReadFactory;
import org.knime.filehandling.core.node.table.reader.ProductionPathProvider;
import org.knime.filehandling.core.node.table.reader.ReadAdapterFactory;
import org.knime.filehandling.core.node.table.reader.TableReader;
import org.knime.filehandling.core.node.table.reader.config.StorableMultiTableReadConfig;
import org.knime.filehandling.core.node.table.reader.paths.SourceSettings;
import org.knime.filehandling.core.node.table.reader.preview.dialog.AbstractTableReaderNodeDialog;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeHierarchy;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.xml.sax.SAXException;

/**
 * Factory for the Delta Table Reader node.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
public final class IcebergTableReaderNodeFactory
    extends AbstractTableReaderNodeFactory<IcebergReaderConfig, IcebergDataType, IcebergValue>
    implements NodeDialogFactory {

    private static final String FULL_DESCRIPTION = """
            <p>
            Reads <a href="https://iceberg.apache.org">Apache Iceberg tables</a> by loading the latest snapshot of the specified
            table.
            </p>
            """;

    private static final String INPUT_PORT_GROUP = "File System Connection";

    private static final String OUTPUT_PORT_GROUP = "Output Table";

    private static final WebUINodeConfiguration CONFIG = WebUINodeConfiguration.builder()//
        .name("Iceberg Table Reader (Labs)")//
        .icon("./icebergread.png") //
        .shortDescription("Read a Iceberg Table")//
        .fullDescription(FULL_DESCRIPTION)//
        .modelSettingsClass(IcebergReaderNodeSettings.class)//
        .nodeType(NodeType.Source)//
        .addInputPort(INPUT_PORT_GROUP, FileSystemPortObject.TYPE, "File system", true)//
        .addOutputTable(OUTPUT_PORT_GROUP, "Iceberg Table") //
        .sinceVersion(5, 5, 0).build();

    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        return WebUINodeFactory.createNodeDescription(CONFIG);
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, IcebergReaderNodeSettings.class);
    }

    @Override
    protected TableReader<IcebergReaderConfig, IcebergDataType, IcebergValue> createReader() {
        return new IcebergReader(false);
    }

    @Override
    protected AbstractTableReaderNodeDialog<FSPath, IcebergReaderConfig, IcebergDataType> createNodeDialogPane(
        final NodeCreationConfiguration creationConfig,
        final MultiTableReadFactory<FSPath, IcebergReaderConfig, IcebergDataType> readFactory,
        final ProductionPathProvider<IcebergDataType> defaultProductionPathFn) {
        // not used
        return null;
    }

    @Override
    protected SourceSettings<FSPath> createPathSettings(final NodeCreationConfiguration nodeCreationConfig) {
        final var settingsModel = new SettingsModelReaderFileChooser("file_selection",
            nodeCreationConfig.getPortConfig().orElseThrow(IllegalStateException::new), FS_CONNECT_GRP_ID,
            EnumConfig.create(FilterMode.FOLDER), EnumSet.complementOf(EnumSet.of(FSCategory.CUSTOM_URL)));
        final Optional<? extends URLConfiguration> urlConfig = nodeCreationConfig.getURLConfig();
        if (urlConfig.isPresent()) {
            settingsModel
                .setLocation(FSLocationUtil.createFromURL(urlConfig.get().getUrl().toString()));
        }
        return settingsModel;
    }

    @Override
    protected ReadAdapterFactory<IcebergDataType, IcebergValue> getReadAdapterFactory() {
        return IcebergReadAdapterFactory.INSTANCE;
    }

    @Override
    protected String extractRowKey(final IcebergValue value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected TypeHierarchy<IcebergDataType, IcebergDataType> getTypeHierarchy() {
        return IcebergReadAdapterFactory.TYPE_HIERARCHY;
    }

    @Override
    protected ProductionPathProvider<IcebergDataType> createProductionPathProvider() {
        return IcebergReadAdapterFactory.INSTANCE.createProductionPathProvider();
    }

    @Override
    protected StorableMultiTableReadConfig<IcebergReaderConfig, IcebergDataType>
        createConfig(final NodeCreationConfiguration nodeCreationConfig) {
        return new IcebergReaderMultiTableReadConfig();
    }

}
