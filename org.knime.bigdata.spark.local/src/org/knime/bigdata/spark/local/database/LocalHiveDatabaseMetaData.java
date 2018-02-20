/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 06.02.2018 by oole
 */
package org.knime.bigdata.spark.local.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hive.jdbc.HiveDatabaseMetaData;
import org.apache.hive.jdbc.HiveQueryResultSet;
import org.knime.bigdata.spark.local.node.create.LocalEnvironmentCreatorNodeFactory;


/**
 * This class wraps the {@link HiveDatabaseMetaData}.
 * It overrides the  {@link HiveDatabaseMetaData#getTables(String, String, String, String[])},
 * to enable metadata collection from the local thrift server instance set up by {@link LocalEnvironmentCreatorNodeFactory}
 *
 * @author Ole Ostergaard, KNIME AG, Konstanz, Germany
 *
 */
public class LocalHiveDatabaseMetaData implements DatabaseMetaData {

	private final HiveDatabaseMetaData m_hiveMetadata;

	LocalHiveDatabaseMetaData(HiveDatabaseMetaData hiveMetadata) {
		m_hiveMetadata = hiveMetadata;
	}

	@Override
	public int hashCode() {
		return m_hiveMetadata.hashCode();
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return m_hiveMetadata.allProceduresAreCallable();
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return m_hiveMetadata.allTablesAreSelectable();
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return m_hiveMetadata.autoCommitFailureClosesAllResultSets();
	}

	@Override
	public boolean equals(Object obj) {
		return m_hiveMetadata.equals(obj);
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return m_hiveMetadata.dataDefinitionCausesTransactionCommit();
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return m_hiveMetadata.dataDefinitionIgnoredInTransactions();
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return m_hiveMetadata.deletesAreDetected(type);
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return m_hiveMetadata.doesMaxRowSizeIncludeBlobs();
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException {
		return m_hiveMetadata.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException {
		return m_hiveMetadata.getBestRowIdentifier(catalog, schema, table, scope, nullable);
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return m_hiveMetadata.getCatalogSeparator();
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return m_hiveMetadata.getCatalogTerm();
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return m_hiveMetadata.getCatalogs();
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return m_hiveMetadata.getClientInfoProperties();
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException {
		return m_hiveMetadata.getColumnPrivileges(catalog, schema, table, columnNamePattern);
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException {
		return m_hiveMetadata.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return m_hiveMetadata.generatedKeyAlwaysReturned();
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		return m_hiveMetadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return m_hiveMetadata.getConnection();
	}

	@Override
	public ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		return m_hiveMetadata.getCrossReference(primaryCatalog, primarySchema, primaryTable, foreignCatalog,
				foreignSchema, foreignTable);
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return m_hiveMetadata.getDatabaseMajorVersion();
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return m_hiveMetadata.getDatabaseMinorVersion();
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return m_hiveMetadata.getDatabaseProductName();
	}

	@Override
	public String toString() {
		return m_hiveMetadata.toString();
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return m_hiveMetadata.getDatabaseProductVersion();
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return m_hiveMetadata.getDefaultTransactionIsolation();
	}

	@Override
	public int getDriverMajorVersion() {
		return m_hiveMetadata.getDriverMajorVersion();
	}

	@Override
	public int getDriverMinorVersion() {
		return m_hiveMetadata.getDriverMinorVersion();
	}

	@Override
	public String getDriverName() throws SQLException {
		return m_hiveMetadata.getDriverName();
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return m_hiveMetadata.getDriverVersion();
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return m_hiveMetadata.getExportedKeys(catalog, schema, table);
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return m_hiveMetadata.getExtraNameCharacters();
	}

	@Override
	public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		return m_hiveMetadata.getFunctionColumns(arg0, arg1, arg2, arg3);
	}

	@Override
	public ResultSet getFunctions(String catalogName, String schemaPattern, String functionNamePattern)
			throws SQLException {
		return m_hiveMetadata.getFunctions(catalogName, schemaPattern, functionNamePattern);
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return m_hiveMetadata.getIdentifierQuoteString();
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return m_hiveMetadata.getImportedKeys(catalog, schema, table);
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException {
		return m_hiveMetadata.getIndexInfo(catalog, schema, table, unique, approximate);
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return m_hiveMetadata.getJDBCMajorVersion();
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return m_hiveMetadata.getJDBCMinorVersion();
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return m_hiveMetadata.getMaxBinaryLiteralLength();
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return m_hiveMetadata.getMaxCatalogNameLength();
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return m_hiveMetadata.getMaxCharLiteralLength();
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return m_hiveMetadata.getMaxColumnNameLength();
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return m_hiveMetadata.getMaxColumnsInGroupBy();
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return m_hiveMetadata.getMaxColumnsInIndex();
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return m_hiveMetadata.getMaxColumnsInOrderBy();
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return m_hiveMetadata.getMaxColumnsInSelect();
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return m_hiveMetadata.getMaxColumnsInTable();
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return m_hiveMetadata.getMaxConnections();
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return m_hiveMetadata.getMaxCursorNameLength();
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return m_hiveMetadata.getMaxIndexLength();
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return m_hiveMetadata.getMaxProcedureNameLength();
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return m_hiveMetadata.getMaxRowSize();
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return m_hiveMetadata.getMaxSchemaNameLength();
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return m_hiveMetadata.getMaxStatementLength();
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return m_hiveMetadata.getMaxStatements();
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return m_hiveMetadata.getMaxTableNameLength();
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return m_hiveMetadata.getMaxTablesInSelect();
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return m_hiveMetadata.getMaxUserNameLength();
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return m_hiveMetadata.getNumericFunctions();
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		return m_hiveMetadata.getPrimaryKeys(catalog, schema, table);
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {
		return m_hiveMetadata.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return m_hiveMetadata.getProcedureTerm();
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			throws SQLException {
		return m_hiveMetadata.getProcedures(catalog, schemaPattern, procedureNamePattern);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return m_hiveMetadata.getResultSetHoldability();
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return m_hiveMetadata.getRowIdLifetime();
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return m_hiveMetadata.getSQLKeywords();
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return m_hiveMetadata.getSQLStateType();
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return m_hiveMetadata.getSchemaTerm();
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return m_hiveMetadata.getSchemas();
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		return m_hiveMetadata.getSchemas(catalog, schemaPattern);
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return m_hiveMetadata.getSearchStringEscape();
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return m_hiveMetadata.getStringFunctions();
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return m_hiveMetadata.getSuperTables(catalog, schemaPattern, tableNamePattern);
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		return m_hiveMetadata.getSuperTypes(catalog, schemaPattern, typeNamePattern);
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return m_hiveMetadata.getSystemFunctions();
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException {
		return m_hiveMetadata.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		return m_hiveMetadata.getTableTypes();
	}

	/**
	 * This overwrite fixes the problems when querying the local thrift server, which is created when using the
	 * {@link LocalEnvironmentCreatorNodeFactory}.
	 */
	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
	    final Statement statement = getConnection().createStatement();
	    ResultSet result = null;
	    if ((types != null && types[0].equals("TABLE") ) || (tableNamePattern != null && types == null)) {
	    	final StringBuilder query = new StringBuilder();
	    	query.append("SHOW TABLES");
	    	if (!tableNamePattern.equals("%")) {
	    		query.append(" like '" + tableNamePattern + "'");
	    	}
		    final HiveQueryResultSet result2 =
		    		(HiveQueryResultSet) statement.executeQuery(query.toString());

		    result = new LocalHiveQueryResultSet(result2);
	    } else {
	    	result = m_hiveMetadata.getTables(catalog, schemaPattern, tableNamePattern, types);
	    }
	    return result;
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return m_hiveMetadata.getTimeDateFunctions();
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		return m_hiveMetadata.getTypeInfo();
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException {
		return m_hiveMetadata.getUDTs(catalog, schemaPattern, typeNamePattern, types);
	}

	@Override
	public String getURL() throws SQLException {
		return m_hiveMetadata.getURL();
	}

	@Override
	public String getUserName() throws SQLException {
		return m_hiveMetadata.getUserName();
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		return m_hiveMetadata.getVersionColumns(catalog, schema, table);
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return m_hiveMetadata.insertsAreDetected(type);
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return m_hiveMetadata.isCatalogAtStart();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return m_hiveMetadata.isReadOnly();
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return m_hiveMetadata.locatorsUpdateCopy();
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return m_hiveMetadata.nullPlusNonNullIsNull();
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return m_hiveMetadata.nullsAreSortedAtEnd();
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return m_hiveMetadata.nullsAreSortedAtStart();
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return m_hiveMetadata.nullsAreSortedHigh();
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return m_hiveMetadata.nullsAreSortedLow();
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return m_hiveMetadata.othersDeletesAreVisible(type);
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return m_hiveMetadata.othersInsertsAreVisible(type);
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return m_hiveMetadata.othersUpdatesAreVisible(type);
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return m_hiveMetadata.ownDeletesAreVisible(type);
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return m_hiveMetadata.ownInsertsAreVisible(type);
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return m_hiveMetadata.ownUpdatesAreVisible(type);
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return m_hiveMetadata.storesLowerCaseIdentifiers();
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return m_hiveMetadata.storesLowerCaseQuotedIdentifiers();
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return m_hiveMetadata.storesMixedCaseIdentifiers();
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return m_hiveMetadata.storesMixedCaseQuotedIdentifiers();
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return m_hiveMetadata.storesUpperCaseIdentifiers();
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return m_hiveMetadata.storesUpperCaseQuotedIdentifiers();
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return m_hiveMetadata.supportsANSI92EntryLevelSQL();
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return m_hiveMetadata.supportsANSI92FullSQL();
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return m_hiveMetadata.supportsANSI92IntermediateSQL();
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return m_hiveMetadata.supportsAlterTableWithAddColumn();
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return m_hiveMetadata.supportsAlterTableWithDropColumn();
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return m_hiveMetadata.supportsBatchUpdates();
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return m_hiveMetadata.supportsCatalogsInDataManipulation();
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return m_hiveMetadata.supportsCatalogsInIndexDefinitions();
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return m_hiveMetadata.supportsCatalogsInPrivilegeDefinitions();
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return m_hiveMetadata.supportsCatalogsInProcedureCalls();
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return m_hiveMetadata.supportsCatalogsInTableDefinitions();
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return m_hiveMetadata.supportsColumnAliasing();
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return m_hiveMetadata.supportsConvert();
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return m_hiveMetadata.supportsConvert(fromType, toType);
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return m_hiveMetadata.supportsCoreSQLGrammar();
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return m_hiveMetadata.supportsCorrelatedSubqueries();
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return m_hiveMetadata.supportsDataDefinitionAndDataManipulationTransactions();
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return m_hiveMetadata.supportsDataManipulationTransactionsOnly();
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return m_hiveMetadata.supportsDifferentTableCorrelationNames();
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return m_hiveMetadata.supportsExpressionsInOrderBy();
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return m_hiveMetadata.supportsExtendedSQLGrammar();
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return m_hiveMetadata.supportsFullOuterJoins();
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return m_hiveMetadata.supportsGetGeneratedKeys();
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return m_hiveMetadata.supportsGroupBy();
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return m_hiveMetadata.supportsGroupByBeyondSelect();
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return m_hiveMetadata.supportsGroupByUnrelated();
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return m_hiveMetadata.supportsIntegrityEnhancementFacility();
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return m_hiveMetadata.supportsLikeEscapeClause();
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return m_hiveMetadata.supportsLimitedOuterJoins();
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return m_hiveMetadata.supportsMinimumSQLGrammar();
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return m_hiveMetadata.supportsMixedCaseIdentifiers();
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return m_hiveMetadata.supportsMixedCaseQuotedIdentifiers();
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return m_hiveMetadata.supportsMultipleOpenResults();
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return m_hiveMetadata.supportsMultipleResultSets();
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return m_hiveMetadata.supportsMultipleTransactions();
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return m_hiveMetadata.supportsNamedParameters();
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return m_hiveMetadata.supportsNonNullableColumns();
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return m_hiveMetadata.supportsOpenCursorsAcrossCommit();
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return m_hiveMetadata.supportsOpenCursorsAcrossRollback();
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return m_hiveMetadata.supportsOpenStatementsAcrossCommit();
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return m_hiveMetadata.supportsOpenStatementsAcrossRollback();
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return m_hiveMetadata.supportsOrderByUnrelated();
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return m_hiveMetadata.supportsOuterJoins();
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return m_hiveMetadata.supportsPositionedDelete();
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return m_hiveMetadata.supportsPositionedUpdate();
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		return m_hiveMetadata.supportsResultSetConcurrency(type, concurrency);
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		return m_hiveMetadata.supportsResultSetHoldability(holdability);
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return m_hiveMetadata.supportsResultSetType(type);
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return m_hiveMetadata.supportsSavepoints();
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return m_hiveMetadata.supportsSchemasInDataManipulation();
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return m_hiveMetadata.supportsSchemasInIndexDefinitions();
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return m_hiveMetadata.supportsSchemasInPrivilegeDefinitions();
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return m_hiveMetadata.supportsSchemasInProcedureCalls();
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return m_hiveMetadata.supportsSchemasInTableDefinitions();
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return m_hiveMetadata.supportsSelectForUpdate();
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return m_hiveMetadata.supportsStatementPooling();
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return m_hiveMetadata.supportsStoredFunctionsUsingCallSyntax();
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return m_hiveMetadata.supportsStoredProcedures();
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return m_hiveMetadata.supportsSubqueriesInComparisons();
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return m_hiveMetadata.supportsSubqueriesInExists();
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return m_hiveMetadata.supportsSubqueriesInIns();
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return m_hiveMetadata.supportsSubqueriesInQuantifieds();
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return m_hiveMetadata.supportsTableCorrelationNames();
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return m_hiveMetadata.supportsTransactionIsolationLevel(level);
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return m_hiveMetadata.supportsTransactions();
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return m_hiveMetadata.supportsUnion();
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return m_hiveMetadata.supportsUnionAll();
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return m_hiveMetadata.updatesAreDetected(type);
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return m_hiveMetadata.usesLocalFilePerTable();
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return m_hiveMetadata.usesLocalFiles();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return m_hiveMetadata.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return m_hiveMetadata.unwrap(iface);
	}
}
