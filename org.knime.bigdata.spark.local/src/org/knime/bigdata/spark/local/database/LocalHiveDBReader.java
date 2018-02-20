package org.knime.bigdata.spark.local.database;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.reader.DBReader;
import org.knime.core.node.port.database.reader.DBReaderImpl;
import org.knime.core.node.streamable.BufferedDataTableRowOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.workflow.CredentialsProvider;

public class LocalHiveDBReader implements DBReader {

	private final DBReaderImpl m_dbreader;

	LocalHiveDBReader(DatabaseQueryConnectionSettings conn) {
		m_dbreader = new DBReaderImpl(conn);
	}

	/**
     * Returns the database meta data on the connection.
     * @param cp CredentialsProvider to receive user/password from
     * @return DatabaseMetaData on this connection
     * @throws SQLException if the connection to the database or the statement
     *         could not be created
     */
    @Override
    public final DatabaseMetaData getDatabaseMetaData(
            final CredentialsProvider cp) throws SQLException {
        try {
            final DatabaseQueryConnectionSettings dbConn = getQueryConnection();

            final Connection conn = dbConn.createConnection(cp);
            synchronized (dbConn.syncConnection(conn)) {
            	final DatabaseMetaData metaData = conn.getMetaData();
                return metaData;
            }
        } catch (final SQLException sql) {
            throw sql;
        } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException | InvalidSettingsException
                | IOException ex) {
            throw new SQLException(ex);
        }
    }


	@Override
	public int hashCode() {
		return m_dbreader.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return m_dbreader.equals(obj);
	}

	@Override
	public DatabaseQueryConnectionSettings getQueryConnection() {
		return m_dbreader.getQueryConnection();
	}

	@Override
	public void updateQuery(String query) {
		m_dbreader.updateQuery(query);
	}

	@Override
	public DataTableSpec getDataTableSpec(CredentialsProvider cp) throws SQLException {
		return m_dbreader.getDataTableSpec(cp);
	}

	@Override
	public String toString() {
		return m_dbreader.toString();
	}

	@Override
	public BufferedDataTable createTable(ExecutionContext exec, CredentialsProvider cp, boolean useDbRowId)
			throws CanceledExecutionException, SQLException {
		return m_dbreader.createTable(exec, cp, useDbRowId);
	}

	@Override
	public DataTable getTable(ExecutionMonitor exec, CredentialsProvider cp, boolean useDbRowId, int cachedNoRows)
			throws CanceledExecutionException, SQLException {
		return m_dbreader.getTable(exec, cp, useDbRowId, cachedNoRows);
	}

	@Override
	public BufferedDataTableRowOutput loopTable(ExecutionContext exec, CredentialsProvider cp, RowInput data,
			long rowCount, boolean failIfException, boolean appendInputColumns, boolean includeEmptyResults,
			boolean retainAllColumns, String... columns) throws Exception {
		return m_dbreader.loopTable(exec, cp, data, rowCount, failIfException, appendInputColumns, includeEmptyResults,
				retainAllColumns, columns);
	}

	@Override
	public BufferedDataTable getErrorDataTable() {
		return m_dbreader.getErrorDataTable();
	}
}
