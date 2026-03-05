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
 *   Mar 5, 2026 (Halil Yerlikaya, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.database.impala.node;

import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;
import org.knime.database.port.DBSessionPortObject;

/**
 *
 * @author Halil Yerlikaya, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
public class ImpalaConnectorNodeFactory2 extends WebUINodeFactory<ImpalaConnectorNodeModel2> {

    /**
     *
     */
    public ImpalaConnectorNodeFactory2() {
        super(CONFIGURATION);
    }

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder() //
        .name("Impala Connector") //
        .icon("impala_connector.png") //
        .shortDescription("Create a database connection to Apache Impala™.") //
        .fullDescription("""
                This node creates a connection to Apache Impala™ via JDBC. You need to provide the server's \
                hostname (or IP address), the port, and a database name. Login credentials can either be \
                provided directly in the configuration or via credential variables.
                """) //
        .modelSettingsClass(ImpalaConnectorNodeSettings.class) //
        .addExternalResource("https://docs.knime.com/latest/db_extension_guide/index.html#register_jdbc",
            "Database documentation") //
        .nodeType(NodeType.Source) //
        .addOutputPort("DB Connection", DBSessionPortObject.TYPE, "Impala DB Connection.") //
        .keywords("database", "db", "connection", "session", "create", "impala") //
        .build();

    @Override
    public ImpalaConnectorNodeModel2 createNodeModel() {
        return new ImpalaConnectorNodeModel2(CONFIGURATION);
    }
}
