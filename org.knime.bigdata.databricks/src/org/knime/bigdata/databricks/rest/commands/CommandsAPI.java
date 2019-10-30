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
 */
package org.knime.bigdata.databricks.rest.commands;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * REST 1.2 API definition of Databricks commands API.
 *
 * @see <a href="https://docs.databricks.com/dev-tools/api/1.2/index.html#execution-commands">Commands API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
@Path("1.2/commands")
public interface CommandsAPI {

    /**
     * Execute a command in a context.
     * @param req command to execute
     * @return id of the command
     * @throws IOException on failures
     */
    @POST
    @Path("execute")
    Command execute(CommandExecute req) throws IOException;

    /**
     * Get current status of a given context
     *
     * @param clusterId The cluster containing the context.
     * @param contextId The context running the command.
     * @param commandId The command about which to retrieve information.
     * @return informations about the context
     * @throws IOException on failures
     */
    @GET
    @Path("status")
    Command status(@QueryParam("clusterId") String clusterId, @QueryParam("contextId") String contextId, @QueryParam("commandId") String commandId) throws IOException;

    /**
     * Cancel a command.
     *
     * @param req command to cancel request
     * @return informations about the command
     * @throws IOException on failures
     */
    @POST
    @Path("cancel")
    Command cancel(CommandCancel req) throws IOException;
}
