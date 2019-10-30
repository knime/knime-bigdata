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
package org.knime.bigdata.databricks.rest.jobs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Informations about a job run. Contains only parts of the description. Some fields might be empty.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Run {

    /**
     * The canonical identifier for the newly submitted run.
     */
    public long run_id;

    /**
     * The result and lifecycle states of the run.
     */
    public RunState state;

    /**
     * The time at which this run was started in epoch milliseconds (milliseconds since 1/1/1970 UTC). This may not be
     * the time when the job task starts executing, for example, if the job is scheduled to run on a new cluster, this
     * is the time the cluster creation call is issued.
     */
    public long start_time;

    /**
     * The time it took to set up the cluster in milliseconds. For runs that run on new clusters this is the cluster
     * creation time, for runs that run on existing clusters this time should be very short.
     */
    public long setup_duration;

    /**
     * The time in milliseconds it took to execute the commands in the JAR or notebook until they completed, failed,
     * timed out, were cancelled, or encountered an unexpected error.
     */
    public long execution_duration;

    /**
     * The time in milliseconds it took to terminate the cluster and clean up any intermediary results, etc. The total
     * duration of the run is the sum of the setup_duration, the execution_duration, and the cleanup_duration.
     */
    public long cleanup_duration;
}
