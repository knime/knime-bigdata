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
 *   2024-05-24 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.unity.filehandling.node;

import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObject;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;
import org.knime.filehandling.core.port.FileSystemPortObject;

/**
 * Factory for the Databricks Unity File System Connector.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
public class UnityFileSystemConnectorNodeFactory extends WebUINodeFactory<UnityFileSystemConnectorNodeModel> {

    private static final String FULL_DESCRIPTION =
        "<p>Connects to the Unity Volumes of a Databricks workspace, in order to read/write files in downstream"//
            + "nodes.</p>"//
            + "<p>\n"//
            + "The resulting\n"//
            + "output port allows downstream nodes to access Databricks Unity Volumes as a file system, e.g.\n"//
            + "to read or write files and folders, or to perform other file system operations (browse/list\n"//
            + "files, copy, move, ...).\n"//
            + "</p>\n"//
            + "<p><b>Path syntax:</b> Paths for this connector are specified with a UNIX-like syntax, for example\n"//
            + "<tt>/example-catalog/example-schema/example-volume/myfolder/file.csv</tt>, which is an absolute path "
            + "that consists of:\n"//
            + "<ol>\n"//
            + "    <li>a leading slash (<tt>/</tt>)</li>\n"//
            + "    <li>the name of a catalog in the workspace (<tt>example-catalog</tt>)</li>\n"//
            + "    <li>the name of a schema in the catalog (<tt>example-schema</tt>)</li>\n"//
            + "    <li>the name of a volume in the schema (<tt>example-volume</tt>)</li>\n"//
            + "    <li>The name of a folder or file in the volume (<tt>myfolder/file.csv</tt>)</li>\n"//
            + "</ol>\n"//
            + "</p>";

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()//
        .name("Databricks Unity File System Connector") //
        .icon("./file_system_connector.png") //
        .shortDescription("Databricks Unity File System Connector node.") //
        .fullDescription(FULL_DESCRIPTION) //
        .modelSettingsClass(UnityFileSystemConnectorSettings.class) //
        .nodeType(NodeType.Source)//
        .addInputPort("Databricks Workspace Connection", //
            DatabricksWorkspacePortObject.TYPE, //
            "Databricks Workspace connection") //
        .addOutputPort("File System", FileSystemPortObject.TYPE, "Databricks Unity File System.") //
        .sinceVersion(5, 3, 0)//
        .build();

    /**
     * Creates new instance.
     */
    public UnityFileSystemConnectorNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public UnityFileSystemConnectorNodeModel createNodeModel() {
        return new UnityFileSystemConnectorNodeModel(CONFIGURATION);
    }
}
