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
package org.knime.bigdata.databricks.sparksql;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import org.knime.bigdata.databricks.rest.clusters.Cluster;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPI;
import org.knime.bigdata.databricks.rest.clusters.ClusterState;
import org.knime.bigdata.databricks.rest.libraries.LibrariesAPI;
import org.knime.bigdata.databricks.rest.libraries.LibraryStatus;
import org.knime.bigdata.databricks.rest.libraries.UnInstallLibrary;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContext;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

import jakarta.ws.rs.ClientErrorException;

/**
 * Provides utility method to start a Databricks compute cluster.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
final class ClusterUtil {

    private static final NodeLogger LOG = NodeLogger.getLogger(ClusterUtil.class);

    /** Timeout for waiting for the cluster to reach RUNNING state (15 minutes). */
    private static final long START_CLUSTER_TIMEOUT_MS = 900 * 1000L;

    /** Polling interval when waiting for cluster state changes. */
    private static final long POLL_INTERVAL_MS = 5000L;

    private ClusterUtil() {
        // utility class
    }

    /**
     * Ensures that the given cluster is in {@link ClusterState#RUNNING} state. If the cluster is
     * {@link ClusterState#TERMINATED}, any previously installed KNIME job jar is removed and the cluster is started.
     * The method then polls the cluster status until it reaches {@link ClusterState#RUNNING} or a timeout occurs.
     *
     * @param clusterAPI the cluster REST API client
     * @param librariesAPI the libraries REST API client
     * @param clusterId the ID of the cluster to ensure is running
     * @param exec execution monitor for cancellation support; may be {@code null}
     * @throws IOException on connection failures
     * @throws CanceledExecutionException if the operation was canceled
     * @throws InterruptedException if the thread was interrupted while waiting
     */
    static void ensureClusterRunning(final ClusterAPI clusterAPI, final LibrariesAPI librariesAPI,
        final String clusterId, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException, InterruptedException {

        var clusterInfo = clusterAPI.getCluster(clusterId);

        switch (clusterInfo.state) {
            case PENDING:
            case RUNNING:
            case RESTARTING:
            case RESIZING:
                // Cluster is already starting or running – nothing to do except wait for RUNNING below.
                break;

            case TERMINATED:
                // Remove a possible old KNIME job jar to avoid an additional cluster restart after start-up.
                removeJobJar(librariesAPI, clusterId);

                LOG.info("Starting compute cluster " + clusterId + " on Databricks.");
                if (exec != null) {
                    exec.setMessage("Starting cluster " + clusterId);
                }
                clusterAPI.start(Cluster.create(clusterId));
                break;

            case ERROR:
            case TERMINATING:
            case UNKNOWN:
                throw new IOException("Cluster " + clusterId + " is in a non-startable state: " + clusterInfo.state);
        }

        // Poll until the cluster is RUNNING or the timeout is exceeded.
        final long deadline = System.currentTimeMillis() + START_CLUSTER_TIMEOUT_MS;
        while (clusterInfo.state != ClusterState.RUNNING && System.currentTimeMillis() < deadline) {
            sleep(POLL_INTERVAL_MS, exec);
            clusterInfo = clusterAPI.getCluster(clusterId);
        }

        if (clusterInfo.state != ClusterState.RUNNING) {
            throw new IOException("Timeout waiting for cluster " + clusterId
                + " to reach RUNNING state (current state: " + clusterInfo.state + ")");
        }

        LOG.info("Cluster " + clusterId + " on Databricks is in state " + clusterInfo.state);
    }

    /**
     * Removes any previously installed KNIME job jar from the given cluster. This is done so that a subsequent cluster
     * start does not trigger an unnecessary restart due to a stale library.
     */
    private static void removeJobJar(final LibrariesAPI librariesAPI, final String clusterId) throws IOException {
        final var jobJarStatus = findJobJarOnCluster(librariesAPI, clusterId);
        if (jobJarStatus.isPresent()) {
            LOG.debug("Uninstalling job jar " + jobJarStatus.get().library.jar + " from cluster " + clusterId);
            librariesAPI.uninstall(UnInstallLibrary.create(clusterId, Arrays.asList(jobJarStatus.get().library)));
        }
    }

    /**
     * Searches for a jar library whose name ends with {@link DatabricksSparkContext#JOB_JAR_FILENAME} on the given
     * cluster.
     *
     * @return the matching {@link LibraryStatus}, or {@code null} if none was found
     */
    private static Optional<LibraryStatus> findJobJarOnCluster(final LibrariesAPI librariesAPI, final String clusterId)
        throws IOException {

        try {
            final var clusterStatus = librariesAPI.getClusterStatus(clusterId);
            if (clusterStatus.library_statuses != null) {
                for (final var libStatus : clusterStatus.library_statuses) {
                    if (libStatus.library.jar != null && libStatus.library.jar.endsWith(DatabricksSparkContext.JOB_JAR_FILENAME)) {
                        return Optional.of(libStatus);
                    }
                }
            }
        } catch (final ClientErrorException e) { // NOSONAR – cluster may simply have no libraries
            LOG.debug("No library status available for cluster " + clusterId);
        }
        return Optional.empty();
    }

    /**
     * Sleeps for the given duration while periodically checking for cancellation.
     *
     * @param timeToWait time to wait in milliseconds
     * @param exec execution monitor to check for cancellation; may be {@code null}
     * @throws CanceledExecutionException if the execution was canceled
     * @throws InterruptedException if the thread was interrupted
     */
    private static void sleep(final long timeToWait, final ExecutionMonitor exec)
        throws CanceledExecutionException, InterruptedException {

        final long end = System.currentTimeMillis() + timeToWait;
        while (System.currentTimeMillis() < end) {
            if (exec != null) {
                exec.checkCanceled();
            }
            Thread.sleep(250);
        }
    }
}
