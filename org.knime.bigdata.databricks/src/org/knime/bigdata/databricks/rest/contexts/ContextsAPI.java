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
package org.knime.bigdata.databricks.rest.contexts;

import java.io.IOException;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

/**
 * REST 1.2 API definition of Databricks contexts API.
 *
 * @see <a href="https://docs.databricks.com/dev-tools/api/1.2/index.html#execution-context">Contexts API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
@Path("1.2/contexts")
public interface ContextsAPI {

    /**
     * Create a context.
     *
     * @param req Context create request.
     * @return context id of the new context
     * @throws IOException on failures
     */
    @POST
    @Path("create")
    Context create(ContextCreate req) throws IOException;

    /**
     * Get current status of a given context
     *
     * @param clusterId The cluster containing the context.
     * @param contextId The context about which to retrieve information.
     * @return informations about the context
     * @throws IOException on failures
     */
    @GET
    @Path("status")
    Context status(@QueryParam("clusterId") String clusterId, @QueryParam("contextId") String contextId) throws IOException;

    /**
     * Destroy a given context
     *
     * @param req Context destroy request.
     * @return informations about the context
     * @throws IOException on failures
     */
    @POST
    @Path("destroy")
    Context destroy(ContextDestroy req) throws IOException;
}
