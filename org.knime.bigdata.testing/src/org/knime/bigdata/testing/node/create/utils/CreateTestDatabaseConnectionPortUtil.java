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
package org.knime.bigdata.testing.node.create.utils;

import static org.knime.bigdata.testing.node.create.utils.CreateTestSparkContextPortUtil.isLocalSparkWithThriftserver;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.knime.bigdata.database.databricks.testing.DatabricksTestingDatabaseConnectionSettingsFactory;
import org.knime.bigdata.hive.testing.TestingDatabaseConnectionSettingsFactory;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.local.testing.LocalHiveTestingConnectionSettingsFactory;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Utility to create {@link DatabaseConnectionPortObject} based Hive output ports.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateTestDatabaseConnectionPortUtil implements CreateTestPortUtil {

    /**
     * Output test port type of this utility.
     */
    public static final PortType PORT_TYPE = DatabaseConnectionPortObject.TYPE;

    private DatabaseConnectionSettings m_dbSettings;

    @Override
    public PortObjectSpec configure(final SparkContextIDScheme sparkScheme, // NOSONAR allow four return statements
        final Map<String, FlowVariable> flowVars) throws InvalidSettingsException {
        switch (sparkScheme) {
            case SPARK_LOCAL:
                m_dbSettings = null;
                if (isLocalSparkWithThriftserver(flowVars)) {
                    // Hive is only available *after* creating the local Spark context
                    return null;
                } else {
                    return InactiveBranchPortObjectSpec.INSTANCE;
                }
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                m_dbSettings = TestingDatabaseConnectionSettingsFactory.createHiveSettings(flowVars);
                return new DatabaseConnectionPortObjectSpec(m_dbSettings);
            case SPARK_DATABRICKS:
                m_dbSettings = DatabricksTestingDatabaseConnectionSettingsFactory.createDeprecatedDBSettings(flowVars);
                return new DatabaseConnectionPortObjectSpec(m_dbSettings);
            default:
                throw new InvalidSettingsException("Spark context ID scheme not supported: " + sparkScheme);
        }
    }

    @Override
    public PortObject execute(final SparkContextIDScheme sparkScheme, final Map<String, FlowVariable> flowVars,
        final ExecutionContext exec, final CredentialsProvider credentialsProvider) throws Exception {

        switch (sparkScheme) {
            case SPARK_LOCAL:
                if (isLocalSparkWithThriftserver(flowVars)) {
                    final DatabaseConnectionPortObject dbPortObject =
                        new DatabaseConnectionPortObject(new DatabaseConnectionPortObjectSpec(
                            LocalHiveTestingConnectionSettingsFactory.createConnectionSettings(flowVars)));
                    openHiveConnection(dbPortObject.getSpec(), credentialsProvider, exec.createSubProgress(0.1));
                    return dbPortObject;
                } else {
                    return InactiveBranchPortObject.INSTANCE;
                }
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
            case SPARK_DATABRICKS:
                return new DatabaseConnectionPortObject(new DatabaseConnectionPortObjectSpec(m_dbSettings));
            default:
                throw new InvalidSettingsException("Spark context ID scheme not supported: " + sparkScheme);
        }
    }

    private static void openHiveConnection(final DatabaseConnectionPortObjectSpec spec, final CredentialsProvider cp,
        final ExecutionMonitor exec) throws InvalidSettingsException, SQLException {

        exec.setProgress(0, "Opening Hive connection");
        try {
            spec.getConnectionSettings(cp).execute(cp, Objects::nonNull);
            exec.setProgress(1);
        } catch (SQLException ex) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            if (cause == null || cause.getMessage() == null) {
                cause = ex;
            }

            throw new SQLException("Could not create connection to database: " + cause.getMessage(), ex);
        }
    }
}
