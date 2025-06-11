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
package org.knime.bigdata.delta.nodes.reader;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;

import org.apache.xmlbeans.XmlException;
import org.knime.bigdata.delta.nodes.reader.framework.DeltaTableReader;
import org.knime.bigdata.delta.nodes.reader.mapper.DeltaTableReadAdapterFactory;
import org.knime.bigdata.delta.types.DeltaTableDataType;
import org.knime.bigdata.delta.types.DeltaTableValue;
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
public final class DeltaTableReaderNodeFactory
    extends AbstractTableReaderNodeFactory<DeltaTableReaderConfig, DeltaTableDataType, DeltaTableValue>
    implements NodeDialogFactory {

    private static final String FULL_DESCRIPTION = """
            <p>Reader for Delta Tables.</p>
            <p>
            Use this node to read <a href="https://docs.delta.io">Delta Lake tables</a>.
            It reads the latest table snapshot and optionally supports a Filesystem input port, such as OneLake.
            Complex nested structures are not supported.
            </p>""";

    private static final String INPUT_PORT_GROUP = "File System Connection";

    private static final String OUTPUT_PORT_GROUP = "Output Table";

    private static final WebUINodeConfiguration CONFIG = WebUINodeConfiguration.builder()//
        .name("Delta Table Reader (Labs)")//
        .icon("./deltaread.png") //
        .shortDescription("Read a Delta Table")//
        .fullDescription(FULL_DESCRIPTION)//
        .modelSettingsClass(DeltaTableReaderNodeSettings.class)//
        .nodeType(NodeType.Source)//
        .addInputPort(INPUT_PORT_GROUP, FileSystemPortObject.TYPE, "File system", true)//
        .addOutputTable(OUTPUT_PORT_GROUP, "Delta Table") //
        .sinceVersion(5, 5, 0).build();

    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        return WebUINodeFactory.createNodeDescription(CONFIG);
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, DeltaTableReaderNodeSettings.class);
    }

    @Override
    protected TableReader<DeltaTableReaderConfig, DeltaTableDataType, DeltaTableValue> createReader() {
        return new DeltaTableReader(false);
    }

    @Override
    protected AbstractTableReaderNodeDialog<FSPath, DeltaTableReaderConfig, DeltaTableDataType> createNodeDialogPane(
        final NodeCreationConfiguration creationConfig,
        final MultiTableReadFactory<FSPath, DeltaTableReaderConfig, DeltaTableDataType> readFactory,
        final ProductionPathProvider<DeltaTableDataType> defaultProductionPathFn) {
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
    protected ReadAdapterFactory<DeltaTableDataType, DeltaTableValue> getReadAdapterFactory() {
        return DeltaTableReadAdapterFactory.INSTANCE;
    }

    @Override
    protected String extractRowKey(final DeltaTableValue value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected TypeHierarchy<DeltaTableDataType, DeltaTableDataType> getTypeHierarchy() {
        return DeltaTableReadAdapterFactory.TYPE_HIERARCHY;
    }

    @Override
    protected ProductionPathProvider<DeltaTableDataType> createProductionPathProvider() {
        return DeltaTableReadAdapterFactory.INSTANCE.createProductionPathProvider();
    }

    @Override
    protected StorableMultiTableReadConfig<DeltaTableReaderConfig, DeltaTableDataType>
        createConfig(final NodeCreationConfiguration nodeCreationConfig) {
        return new DeltaTableReaderMultiTableReadConfig();
    }

}
