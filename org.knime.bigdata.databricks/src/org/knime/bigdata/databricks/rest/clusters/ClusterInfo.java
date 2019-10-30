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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Describe all of the metadata about a cluster. This class contains only parts of the returned informations.
 *
 * @see <a href="https://docs.databricks.com/api/latest/clusters.html#clusterinfo">Cluster API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterInfo {

    /**
     * Canonical identifier for the cluster. This ID is retained during cluster restarts and resizes, while each new
     * cluster has a globally unique ID.
     */
    public String cluster_id;

    /**
     * Cluster name requested by the user. This doesn’t have to be unique. If not specified at creation, the cluster
     * name will be an empty string.
     */
    public String cluster_name;

    /**
     * Creator user name. The field won’t be included in the response if the user has already been deleted.
     */
    public String creator_user_name;

    /**
     * Port on which Spark JDBC server is listening, in the driver nod. No service will be listening on on this port in
     * executor nodes.
     */
    public int jdbc_port;

    /**
     * The Spark version of the cluster. A list of available Spark versions can be retrieved by using the Spark Versions
     * API call.
     */
    public String spark_version;

    /**
     * Current state of the cluster.
     */
    public ClusterState state;

    /**
     * Time (in epoch milliseconds) when the cluster was last active.
     */
    public long last_activity_time;

    @Override
    public String toString() {
        return cluster_name + " (" + cluster_id + ")";
    }
}
