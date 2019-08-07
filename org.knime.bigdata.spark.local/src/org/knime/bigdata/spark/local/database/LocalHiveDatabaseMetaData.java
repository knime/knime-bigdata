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

import org.apache.commons.lang3.StringUtils;
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

	/**
	 * Creates a LocalHiveMetaDataObject that wraps the HiveDataBaseMetData
	 * @param hiveMetadata
	 */
	public LocalHiveDatabaseMetaData(final HiveDatabaseMetaData hiveMetadata) {
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
	public boolean equals(final Object obj) {
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
	public boolean deletesAreDetected(final int type) throws SQLException {
		return m_hiveMetadata.deletesAreDetected(type);
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return m_hiveMetadata.doesMaxRowSizeIncludeBlobs();
	}

	@Override
	public ResultSet getAttributes(final String catalog, final String schemaPattern, final String typeNamePattern,
			final String attributeNamePattern) throws SQLException {
		return m_hiveMetadata.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
	}

	@Override
	public ResultSet getBestRowIdentifier(final String catalog, final String schema, final String table, 
			final int scope, final boolean nullable) throws SQLException {
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
	public ResultSet getColumnPrivileges(final String catalog, final String schema, final String table, 
			final String columnNamePattern)
			throws SQLException {
		return m_hiveMetadata.getColumnPrivileges(catalog, schema, table, columnNamePattern);
	}

	@Override
	public ResultSet getPseudoColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String columnNamePattern) throws SQLException {
		return m_hiveMetadata.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return m_hiveMetadata.generatedKeyAlwaysReturned();
	}

	@Override
	public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern, 
			final String columnNamePattern) throws SQLException {
		return m_hiveMetadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return m_hiveMetadata.getConnection();
	}

	@Override
	public ResultSet getCrossReference(final String primaryCatalog, final String primarySchema, 
			final String primaryTable, final String foreignCatalog, final String foreignSchema, 
			final String foreignTable) throws SQLException {
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
	public ResultSet getExportedKeys(final String catalog, final String schema, final String table) 
			throws SQLException {
		return m_hiveMetadata.getExportedKeys(catalog, schema, table);
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return m_hiveMetadata.getExtraNameCharacters();
	}

	@Override
	public ResultSet getFunctionColumns(final String arg0, final String arg1, final String arg2, final String arg3) 
			throws SQLException {
		return m_hiveMetadata.getFunctionColumns(arg0, arg1, arg2, arg3);
	}

	@Override
	public ResultSet getFunctions(final String catalogName, final String schemaPattern, 
			final String functionNamePattern) throws SQLException {
		return m_hiveMetadata.getFunctions(catalogName, schemaPattern, functionNamePattern);
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return m_hiveMetadata.getIdentifierQuoteString();
	}

	@Override
	public ResultSet getImportedKeys(final String catalog, final String schema, final String table) 
			throws SQLException {
		return m_hiveMetadata.getImportedKeys(catalog, schema, table);
	}

	@Override
	public ResultSet getIndexInfo(final String catalog, final String schema, final String table, final boolean unique, 
			final boolean approximate) throws SQLException {
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
	public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
		return m_hiveMetadata.getPrimaryKeys(catalog, schema, table);
	}

	@Override
	public ResultSet getProcedureColumns(final String catalog, final String schemaPattern, 
			final String procedureNamePattern, final String columnNamePattern) throws SQLException {
		return m_hiveMetadata.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return m_hiveMetadata.getProcedureTerm();
	}

	@Override
	public ResultSet getProcedures(final String catalog, final String schemaPattern, final String procedureNamePattern)
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
	    try {
	        return m_hiveMetadata.getSQLKeywords();
	    } catch (final Exception e) {
	        //method not supported by driver return empty string instead
	        return StringUtils.EMPTY;
	    }
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
	public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
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
	public ResultSet getSuperTables(final String catalog, final String schemaPattern, final String tableNamePattern) 
			throws SQLException {
		return m_hiveMetadata.getSuperTables(catalog, schemaPattern, tableNamePattern);
	}

	@Override
	public ResultSet getSuperTypes(final String catalog, final String schemaPattern, final String typeNamePattern) 
			throws SQLException {
		return m_hiveMetadata.getSuperTypes(catalog, schemaPattern, typeNamePattern);
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return m_hiveMetadata.getSystemFunctions();
	}

	@Override
	public ResultSet getTablePrivileges(final String catalog, final String schemaPattern, final String tableNamePattern)
			throws SQLException {
		return m_hiveMetadata.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		return m_hiveMetadata.getTableTypes();
	}

	/**
	 * This override fixes the problems when querying the local thrift server, which is created when using the
	 * {@link LocalEnvironmentCreatorNodeFactory}.
	 */
	@Override
	public ResultSet getTables(final String catalog, String schemaPattern, final String tableNamePattern, 
			final String[] types) throws SQLException {

        if ((types != null && types[0].equals("TABLE")) || (tableNamePattern != null && types == null)) {
            final StringBuilder query = new StringBuilder();
            query.append("SHOW TABLES");

            if (schemaPattern == null || schemaPattern.isEmpty()) {
                schemaPattern = getConnection().getSchema();
            }
            query.append(" IN " + schemaPattern);

            if (tableNamePattern != null && !tableNamePattern.equals("%")) {
                query.append(String.format(" LIKE '%s'", tableNamePattern));
            }

            return new LocalHiveQueryResultSet((HiveQueryResultSet) getConnection().createStatement().executeQuery(query.toString()));
        } else {
            return m_hiveMetadata.getTables(catalog, schemaPattern, tableNamePattern, types);
        }
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
	public ResultSet getUDTs(final String catalog, final String schemaPattern, final String typeNamePattern, 
			final int[] types) throws SQLException {
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
	public ResultSet getVersionColumns(final String catalog, final String schema, final String table) 
			throws SQLException {
		return m_hiveMetadata.getVersionColumns(catalog, schema, table);
	}

	@Override
	public boolean insertsAreDetected(final int type) throws SQLException {
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
	public boolean othersDeletesAreVisible(final int type) throws SQLException {
		return m_hiveMetadata.othersDeletesAreVisible(type);
	}

	@Override
	public boolean othersInsertsAreVisible(final int type) throws SQLException {
		return m_hiveMetadata.othersInsertsAreVisible(type);
	}

	@Override
	public boolean othersUpdatesAreVisible(final int type) throws SQLException {
		return m_hiveMetadata.othersUpdatesAreVisible(type);
	}

	@Override
	public boolean ownDeletesAreVisible(final int type) throws SQLException {
		return m_hiveMetadata.ownDeletesAreVisible(type);
	}

	@Override
	public boolean ownInsertsAreVisible(final int type) throws SQLException {
		return m_hiveMetadata.ownInsertsAreVisible(type);
	}

	@Override
	public boolean ownUpdatesAreVisible(final int type) throws SQLException {
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
	public boolean supportsConvert(final int fromType, final int toType) throws SQLException {
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
	public boolean supportsResultSetConcurrency(final int type, final int concurrency) throws SQLException {
		return m_hiveMetadata.supportsResultSetConcurrency(type, concurrency);
	}

	@Override
	public boolean supportsResultSetHoldability(final int holdability) throws SQLException {
		return m_hiveMetadata.supportsResultSetHoldability(holdability);
	}

	@Override
	public boolean supportsResultSetType(final int type) throws SQLException {
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
	public boolean supportsTransactionIsolationLevel(final int level) throws SQLException {
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
	public boolean updatesAreDetected(final int type) throws SQLException {
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
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		return m_hiveMetadata.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException {
		return m_hiveMetadata.unwrap(iface);
	}
}
