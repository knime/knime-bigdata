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
package org.knime.bigdata.databricks.rest.clusters;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * REST API definition of cluster API.
 *
 * @see <a href="https://docs.databricks.com/api/latest/clusters.html#clusters-api">Cluster API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
@Path("2.0/clusters")
public interface ClusterAPI {

    /**
     * Return information about all pinned clusters, currently active clusters, up to 70 of the most recently terminated
     * interactive clusters in the past 30 days, and up to 30 of the most recently terminated job clusters in the past
     * 30 days.
     *
     * @return list of cluster informations
     * @throws IOException on failures
     */
    @GET
    @Path("list")
    ClusterInfoList list() throws IOException;

    /**
     * Retrieve the information for a cluster given its identifier.
     *
     * @param clusterId The cluster about which to retrieve information.
     * @return informations about the cluster
     * @throws IOException on failures
     */
    @GET
    @Path("get")
    ClusterInfo getCluster(@QueryParam("cluster_id") String clusterId) throws IOException;

    /**
     * Start a terminated Spark cluster given its ID. If the cluster is not in a TERMINATED state, nothing will happen.
     *
     * @param cluster The cluster to start.
     * @throws IOException on failures
     */
    @POST
    @Path("start")
    void start(Cluster cluster) throws IOException;

    /**
     * Terminate a Spark cluster given its ID. The cluster is removed asynchronously.
     *
     * @param cluster The cluster to terminate.
     * @throws IOException on failures
     */
    @POST
    @Path("delete")
    void delete(Cluster cluster) throws IOException;
}
