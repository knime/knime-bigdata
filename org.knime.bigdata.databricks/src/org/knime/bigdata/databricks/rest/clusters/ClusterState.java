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
 *   Aug 27, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.databricks.rest.clusters;

/**
 * The state of a cluster. The current allowable state transitions are as follows:
 *   PENDING -> RUNNING
 *   PENDING -> TERMINATING
 *   RUNNING -> RESIZING
 *   RUNNING -> RESTARTING
 *   RUNNING -> TERMINATING
 *   RESTARTING -> RUNNING
 *   RESTARTING -> TERMINATING
 *   RESIZING -> RUNNING
 *   RESIZING -> TERMINATING
 *   TERMINATING -> TERMINATED
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public enum ClusterState {

        /** Indicates that a cluster is in the process of being created. */
        PENDING,

        /** Indicates that a cluster has been started and is ready for use. */
        RUNNING,

        /** Indicates that a cluster is in the process of restarting. */
        RESTARTING,

        /** Indicates that a cluster is in the process of adding or removing nodes. */
        RESIZING,

        /** Indicates that a cluster is in the process of being destroyed. */
        TERMINATING,

        /** Indicates that a cluster has been successfully destroyed. */
        TERMINATED,

        /**
         * This state is not used anymore. It was used to indicate a cluster that failed to be created. Terminating and
         * Terminated are used instead.
         */
        ERROR,

        /** Indicates that a cluster is in an unknown state. A cluster should never be in this state. */
        UNKNOWN
}
