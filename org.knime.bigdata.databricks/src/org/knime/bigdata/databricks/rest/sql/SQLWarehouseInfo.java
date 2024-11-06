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
 *   2024-05-14 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.rest.sql;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata about a Unity Catalog.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SQLWarehouseInfo {

    /**
     * Unique identifier of the warehouse.
     */
    public String id;

    /**
     * Name of the warehouse.
     */
    public String name;

    /**
     * Current state of the warehouse.
     */
    public SQLWarehouseState state;

    /**
     * ODBC connection hostname.
     */
    public String odbcHostname;

    /**
     * ODBC connection HTTP path.
     */
    public String odbcPath;

    /**
     * ODBC connection port.
     */
    public int odbcPort;

    /**
     * Parse ODBC params object.
     */
    @JsonProperty("odbc_params")
    void unpackODBCParams(final Map<String, Object> params) {
        odbcHostname = (String)params.get("hostname");
        odbcPath = (String)params.get("path");
        odbcPort = (Integer)params.get("port");
    }

    /**
     *
     * @return {@code true} if cluster is in running state
     */
    public boolean isClusterRunning() {
        return state == SQLWarehouseState.RUNNING;
    }

}
