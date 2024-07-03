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
 *   17.06.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.databricks.agent.loader;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import org.knime.bigdata.database.loader.BigDataLoaderParameters;
import org.knime.bigdata.database.loader.BigDataLoaderParameters2;
import org.knime.bigdata.database.loader.BigDataLoaderRemoteFS;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.database.SQLCommand;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.dialect.CreateTableParameters;
import org.knime.database.dialect.DBColumn;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.dialect.DBUniqueConstraint;
import org.knime.database.model.DBTable;
import org.knime.database.model.impl.DefaultDBTable;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionReference;

/**
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class DatabricksDataLoader implements DBLoader {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DatabricksDataLoader.class);

    private final DBSessionReference m_sessionReference;

    /**
     * Creates a Databricks Loader
     *
     * @param sessionReference the {@link DBSessionReference} object
     */
    public DatabricksDataLoader(final DBSessionReference sessionReference) {
        m_sessionReference = requireNonNull(sessionReference, "sessionReference");
    }

    /**
     * Gets the dialect.
     *
     * @return the {@link DBSQLDialect} object
     */
    private DBSQLDialect getDialect() {
        return getSession().getDialect();
    }

    /**
     * Gets the session.
     *
     * @return {@link DBSession} object
     */
    private DBSession getSession() {
        return m_sessionReference.get();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void load(final ExecutionMonitor exec, final Object parameters) throws Exception {
        //we need this code for backward compatibility with the deprecated DB Loader implementation
        final DBLoadTableFromFileParameters<?> loadParameters =
            (DBLoadTableFromFileParameters<?>)requireNonNull(parameters, "parameters");
        Object additionalSettings = loadParameters.getAdditionalSettings()
            .orElseThrow(() -> new IllegalStateException("Missing file writer settings."));
        if (additionalSettings instanceof BigDataLoaderParameters) {
            //this is the deprecated DB Loader with the old parameters
            BigDataLoaderRemoteFS deprecatedLoader = new BigDataLoaderRemoteFS(m_sessionReference);
            deprecatedLoader.load(exec, loadParameters);
        } else {
            loadInternal(exec, parameters);
        }
    }

    private void loadInternal(final ExecutionMonitor exec, final Object parameters)
        throws SQLException, CanceledExecutionException {
        @SuppressWarnings("unchecked")
        final DBLoadTableFromFileParameters<BigDataLoaderParameters2> loadParameters =
            (DBLoadTableFromFileParameters<BigDataLoaderParameters2>)requireNonNull(parameters, "parameters");
        if (!loadParameters.getAdditionalSettings().isPresent()) {
            throw new IllegalArgumentException("Missing file writer settings.");
        }
        final BigDataLoaderParameters2 loaderParameters = loadParameters.getAdditionalSettings()
                .orElseThrow(() -> new IllegalStateException("Invalid parameter"));
        final DBTable tmpTable = createTempTable(exec, loadParameters, loaderParameters.getTempTableColumns());
        insertIntoTable(exec, loadParameters, tmpTable, loaderParameters.getSelectOrderColumnNames(),
            loaderParameters.getPartitionColumns());
    }

    private DBTable createTempTable(final ExecutionMonitor exec,
        final DBLoadTableFromFileParameters<BigDataLoaderParameters2> loadParameters, final DBColumn[] columns)
        throws SQLException {
        final String tempTableName =
            loadParameters.getTable().getName() + "_" + UUID.randomUUID().toString().replace('-', '_');
        LOGGER.debug("Creating temporary table " + tempTableName);

        // first create an unpartitioned table
        exec.setProgress(0, "Creating temporary table");
        final DBTable tempTable = new DefaultDBTable(tempTableName, loadParameters.getTable().getSchemaName());

        final SQLCommand createTableCmd = getDialect().dataDefinition().getCreateTableStatement(CreateTableParameters
            .builder(tempTable, columns, new DBUniqueConstraint[0]).build());
        try (final Connection connection = getSession().getConnectionProvider().getConnection(exec);
                final Statement statement = connection.createStatement()) {
            statement.execute(createTableCmd.getSQL());
        } catch (final Throwable throwable) {
            throw new SQLException(throwable.getMessage(), throwable);
        }

        LOGGER.debug("Temporary table sucessful created");
        return tempTable;
    }

    private void insertIntoTable(final ExecutionMonitor exec,
        final DBLoadTableFromFileParameters<BigDataLoaderParameters2> loadParameters, final DBTable tmpTable,
        final List<String> selectOrderColumnNames, final List<DBColumn> partitionColumns)
        throws SQLException, CanceledExecutionException {
        exec.checkCanceled();
        final String tableName = getDialect().createFullName(tmpTable);
        final String filePath = loadParameters.getFilePath();
        final String loadTableCmd = "COPY INTO " + tableName + "\nFROM '" + filePath + "'\nFILEFORMAT = PARQUET";
        final String insertCommand =
            buildInsertCommand(tmpTable, loadParameters.getTable(), selectOrderColumnNames, partitionColumns);
        try (final Connection connection = getSession().getConnectionProvider().getConnection(exec);
                final Statement statement = connection.createStatement()) {
            try {
                exec.setMessage("Importing data to temporary table from uploaded file...");
                exec.checkCanceled();
                statement.execute(loadTableCmd);
                exec.setProgress(0.75, "Data imported into temporary table.");
                exec.setMessage("Loading data into final table...");
                statement.execute(insertCommand);
                exec.setProgress(0.90, "Data loaded into final table.");
            } finally {
                exec.setMessage("Removing temporary table...");
                final SQLCommand dropTableStatement =
                    getDialect().dataDefinition().getDropTableStatement(tmpTable, false);
                statement.execute(dropTableStatement.getSQL());
                exec.setProgress(0.99, "Temporary table removed.");
            }
        } catch (final Throwable throwable) {
            throw new SQLException(throwable.getMessage(), throwable);
        }
    }

    private String buildInsertCommand(final DBTable sourceTable, final DBTable destTable,
        final List<String> selectOrderColumnNames, final List<DBColumn> partitionColumns) {

        DBSQLDialect dialect = getDialect();
        final StringBuilder buf = new StringBuilder();
        buf.append("INSERT INTO TABLE ").append(getDialect().createFullName(destTable));
        if (!partitionColumns.isEmpty()) {
            buf.append(" PARTITION (");
            for (final DBColumn partCol : partitionColumns) {
                buf.append(dialect.delimit(dialect.getUnqualifiedColumnName(partCol.getName()))).append(",");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        buf.append("\n");
        buf.append("SELECT ");
        selectOrderColumnNames.stream()
            .forEachOrdered(col -> buf.append(dialect.getUnqualifiedColumnName(col)).append(","));
        buf.deleteCharAt(buf.length() - 1);
        buf.append("\nFROM ").append(getDialect().createFullName(sourceTable));

        return buf.toString();
    }

}
