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
 *   2024-09-17 (Tobias): created
 */
package org.knime.bigdata.delta.nodes.writer;

import java.io.IOException;
import java.util.Optional;

import org.apache.xmlbeans.XmlException;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.core.webui.node.impl.PortDescription;
import org.knime.core.webui.node.impl.WebUINodeFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public class KNIMETableWriterNodeFactory extends ConfigurableNodeFactory<KNIMETableWriterNodeModel>
    implements NodeDialogFactory {

    /**
     *
     */
    private static final String INPUT_TABLE_GRP_ID = "Input table";
    /** The file system ports group id. */
    protected static final String FS_CONNECT_GRP_ID = "File System Connection";

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() { // only to make this visible to testing
        PortsConfigurationBuilder b = new PortsConfigurationBuilder();
        b.addFixedInputPortGroup(INPUT_TABLE_GRP_ID, BufferedDataTable.TYPE);
        //        b.addOptionalInputPortGroup(FS_CONNECT_GRP_ID, FileSystemPortObject.TYPE);
        return Optional.of(b);
    }

    private static final String FULL_DESCRIPTION = """
            This node reads files that have been written using the Table Writer node (which uses an internal format).
            It retains all meta information such as domain, properties, colors, size.
            """;

    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        return WebUINodeFactory.createNodeDescription("KNIME Data Writer (Labs)", "deltaread.png",
            new PortDescription[]{
                new PortDescription(INPUT_TABLE_GRP_ID, BufferedDataTable.TYPE,
                    "The table contained in the selected Delta table.")},
            new PortDescription[]{},
            //                new PortDescription(FS_CONNECT_GRP_ID, FileSystemPortObject.TYPE, "The file system connection.", true)},
            "Reads Delta tables from a Delta Share.", FULL_DESCRIPTION, KNIMETableWriterNodeSettings.class, null,
            null, NodeType.Source, new String[]{"Delta", "Sharing", "Input", "Read"});
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, KNIMETableWriterNodeSettings.class);
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    protected KNIMETableWriterNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        return new KNIMETableWriterNodeModel(creationConfig.getPortConfig().get());
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<KNIMETableWriterNodeModel> createNodeView(final int viewIndex,
        final KNIMETableWriterNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }
}
