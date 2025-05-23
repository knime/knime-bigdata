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
import java.util.Optional;

import org.apache.xmlbeans.XmlException;
import org.knime.bigdata.delta.nodes.reader.framework.DeltaTableReader;
import org.knime.bigdata.delta.nodes.reader.framework.DeltaTableValue;
import org.knime.bigdata.delta.nodes.reader.mapper.DeltaTableReadAdapterFactory;
import org.knime.core.data.DataType;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.context.url.URLConfiguration;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;
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
import org.knime.filehandling.core.node.table.reader.TableReaderNodeModel;
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
    extends AbstractTableReaderNodeFactory<DeltaTableReaderNodeSettings, DataType, DeltaTableValue>
    implements NodeDialogFactory {

    // TODO: description
    private static final String FULL_DESCRIPTION = """
            <p>Reader for Delta Tables.</p>
            <p>
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
    protected TableReader<DeltaTableReaderNodeSettings, DataType, DeltaTableValue> createReader() {
        return new DeltaTableReader(false);
    }

    @Override
    protected AbstractTableReaderNodeDialog<FSPath, DeltaTableReaderNodeSettings, DataType> createNodeDialogPane(
        final NodeCreationConfiguration creationConfig,
        final MultiTableReadFactory<FSPath, DeltaTableReaderNodeSettings, DataType> readFactory,
        final ProductionPathProvider<DataType> defaultProductionPathFn) {
        // not used
        return null;
    }

    @Override
    protected SourceSettings<FSPath> createPathSettings(final NodeCreationConfiguration nodeCreationConfig) {
        final var settingsModel = new SettingsModelReaderFileChooser("file_selection",
            nodeCreationConfig.getPortConfig().orElseThrow(IllegalStateException::new), FS_CONNECT_GRP_ID,
            EnumConfig.create(FilterMode.FILE, FilterMode.FOLDER)); // TODO: why do we need to support a file here???
        final Optional<? extends URLConfiguration> urlConfig = nodeCreationConfig.getURLConfig();
        if (urlConfig.isPresent()) {
            settingsModel
                .setLocation(FSLocationUtil.createFromURL(urlConfig.get().getUrl().toString()));
        }
        return settingsModel;
    }

    @Override
    protected ReadAdapterFactory<DataType, DeltaTableValue> getReadAdapterFactory() {
        return DeltaTableReadAdapterFactory.INSTANCE;
    }

    @Override
    protected String extractRowKey(final DeltaTableValue value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected TypeHierarchy<DataType, DataType> getTypeHierarchy() {
        return DeltaTableReadAdapterFactory.TYPE_HIERARCHY;
    }

    @Override
    protected ProductionPathProvider<DataType> createProductionPathProvider() {
        return DeltaTableReadAdapterFactory.INSTANCE.createProductionPathProvider();
    }

    @Override
    protected StorableMultiTableReadConfig<DeltaTableReaderNodeSettings, DataType>
        createConfig(final NodeCreationConfiguration nodeCreationConfig) {
        return new DeltaTableReaderMultiTableReadConfig(new DeltaTableReaderNodeSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        // we do not want to use the dialog provided by the framework
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    public TableReaderNodeModel<FSPath, DeltaTableReaderNodeSettings, DataType>
        createNodeModel(final NodeCreationConfiguration creationConfig) {
        final var config = createConfig(creationConfig);
        final var pathSettings = createPathSettings(creationConfig);
        final var reader = createMultiTableReader();
        final var portConfig = creationConfig.getPortConfig();
        if (portConfig.isPresent()) {
            return new DeltaTableReaderNodeModel(config, pathSettings, reader, portConfig.get());
        } else {
            return new DeltaTableReaderNodeModel(config, pathSettings, reader);
        }
    }
}
