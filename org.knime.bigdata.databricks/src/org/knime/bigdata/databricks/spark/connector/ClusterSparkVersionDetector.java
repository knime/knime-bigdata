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
 *   2026-03-12 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.spark.connector;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPI;
import org.knime.bigdata.databricks.rest.clusters.ClusterInfo;
import org.knime.bigdata.databricks.rest.clusters.ClusterSparkVersion;
import org.knime.bigdata.databricks.rest.clusters.ClusterSparkVersionList;
import org.knime.core.node.NodeLogger;

/**
 * Util to detect the Spark version of a Databricks cluster.
 *
 * The utils fetches the cluster information via the Databricks REST API and extracts the Spark version from the runtime
 * name. This only works if the name of the runtime contains the String {@code Spark x.x.}.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
final class ClusterSparkVersionDetector {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ClusterSparkVersionDetector.class);

    private static final Pattern SPARK_VERSION_PATTERN = Pattern.compile("Spark (\\d+\\.\\d+)\\.");

    private ClusterSparkVersionDetector() {
        // prevent instantiation
    }

    /**
     * Load the cluster info, spark-versions and try to extract the spark version from the runtime name.
     *
     * @param client cluster API REST client
     * @param clusterId id of the cluster to retrieve the spark version for
     * @return the detected major and minor spark version, or an empty optional if the version could not be detected
     */
    static Optional<String> getSparkVersion(final ClusterAPI client, final String clusterId) {
        if (StringUtils.isBlank(clusterId)) {
            return Optional.empty();
        }

        try {
            final ClusterInfo clusterInfo = client.getCluster(clusterId);
            final ClusterSparkVersionList sparkVersions = client.listSparkVersions();
            return findSparkVersionName(sparkVersions, clusterInfo.spark_version);
        } catch (final Exception e) {
            LOGGER.error("Failed to retrieve spark version from cluster information for cluster with id " + clusterId,
                e);
            return Optional.empty();
        }
    }

    private static Optional<String> findSparkVersionName(final ClusterSparkVersionList sparkVersions,
        final String runtimeVersion) {

        for (final ClusterSparkVersion version : sparkVersions.versions) {
            if (version.key.equals(runtimeVersion)) {
                return extractSparkVersion(version.name);
            }
        }

        return Optional.empty();
    }

    /**
     * Try to extract the major and minor spark version from the runtime name.
     *
     * @param runtimeName name containing the pattern "Spark x.x." where x is the major and minor version number
     * @return the extracted major and minor spark version, or an empty optional if the version could not be extracted
     */
    private static Optional<String> extractSparkVersion(final String runtimeName) {
        final Matcher matcher = SPARK_VERSION_PATTERN.matcher(runtimeName);
        if (matcher.find()) {
            return Optional.of(matcher.group(1));
        }

        return Optional.empty();
    }

}
