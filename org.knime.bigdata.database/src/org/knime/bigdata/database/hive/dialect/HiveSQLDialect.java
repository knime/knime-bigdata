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
 *   05.04.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.hive.dialect;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.knime.database.SQLCommand;
import org.knime.database.SQLQuery;
import org.knime.database.attribute.Attribute;
import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;
import org.knime.database.dialect.CreateTableParameters;
import org.knime.database.dialect.DBColumn;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.dialect.DBSQLDialectFactory;
import org.knime.database.dialect.DBSQLDialectFactoryParameters;
import org.knime.database.dialect.DBSQLDialectParameters;
import org.knime.database.dialect.impl.SQL92DBSQLDialect;
import org.knime.database.model.DBSchemaObject;

/**
 * Dialect for Hive databases.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 * @since 4.0
 */
public class HiveSQLDialect extends SQL92DBSQLDialect {

    /**
     * {@link DBSQLDialectFactory} that produces {@link HiveSQLDialect} instances.
     *
     * @author  Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class Factory implements DBSQLDialectFactory {
        @Override
        public DBSQLDialect createDialect(final DBSQLDialectFactoryParameters parameters) {
            return new HiveSQLDialect(this,
                new DBSQLDialectParameters(Objects.requireNonNull(parameters, "parameters").getSessionReference()));
        }

        @Override
        public AttributeCollection getAttributes() {
            return ATTRIBUTES;
        }

        @Override
        public String getDescription() {
            return DESCRIPTION;
        }

        @Override
        public String getId() {
            return ID;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    /**
     * Attribute that indicates the capability to define names for {@code CONSTRAINT} definitions in
     * {@code CREATE TABLE} statements.
     */
    @SuppressWarnings("hiding")
    public static final Attribute<Boolean> ATTRIBUTE_CAPABILITY_DEFINE_CREATE_TABLE_CONSTRAINT_NAME;

    /**
     * Attribute that indicates {@code CASE} expression capability.
     */
    @SuppressWarnings("hiding")
    public static final Attribute<Boolean> ATTRIBUTE_CAPABILITY_EXPRESSION_CASE;

    /**
     * Attribute that contains the literal syntax for the {@code IF NOT EXISTS} condition for {@code CREATE TABLE}
     * statements. {@linkplain String#isEmpty() Empty} if the condition is not supported.
     *
     * @see SQL92DBSQLDialect#ATTRIBUTE_SYNTAX_CREATE_TABLE_IF_NOT_EXISTS
     */
    @SuppressWarnings("hiding")
    public static final Attribute<String> ATTRIBUTE_SYNTAX_CREATE_TABLE_IF_NOT_EXISTS;

    /**
     * Attribute that contains the keyword between the table/view name or derived table and the correlation name in
     * table reference expressions.
     */
    @SuppressWarnings("hiding")
    public static final Attribute<String> ATTRIBUTE_SYNTAX_TABLE_REFERENCE_KEYWORD;

    /**
     * Attribute that contains the closing identifier delimiter, e.g.&nbsp;{@code "} or {@code ]}.
     */
    @SuppressWarnings("hiding")
    public static final Attribute<String> ATTRIBUTE_SYNTAX_DELIMITER_IDENTIFIER_CLOSING;

    /**
     * Attribute that contains the opening identifier delimiter, e.g.&nbsp;{@code "} or {@code [}.
     */
    @SuppressWarnings("hiding")
    public static final Attribute<String> ATTRIBUTE_SYNTAX_DELIMITER_IDENTIFIER_OPENING;

    /**
     * Attribute that indicates the capability to drop tables.
     */
    @SuppressWarnings("hiding")
    public static final Attribute<Boolean> ATTRIBUTE_CAPABILITY_DROP_TABLE;

    /**
     * Attribute that indicates the capability of table references being derived tables.
     */
    @SuppressWarnings("hiding")
    public static final Attribute<Boolean> ATTRIBUTE_CAPABILITY_TABLE_REFERENCE_DERIVED_TABLE;

    /**
     * Attribute that indicates the capability to insert into a table via a select statement.
     * @see #createInsertAsSelectStatement(DBSchemaObject, SQLQuery, String...)
     */
    @SuppressWarnings("hiding")
    public static final Attribute<Boolean> ATTRIBUTE_CAPABILITY_INSERT_AS_SELECT;

    /**
     * Attribute that contains the literal keyword or keyword for {@code CREATE [ TEMPORARY ] TABLE}
     * statements.
     */
    @SuppressWarnings("hiding")
    public static final Attribute<String> ATTRIBUTE_SYNTAX_CREATE_TABLE_TEMPORARY;

    /**
     * The {@link AttributeCollection} of this {@link DBSQLDialect}.
     *
     * @see Factory#getAttributes()
     */
    @SuppressWarnings("hiding")
    public static final AttributeCollection ATTRIBUTES;

    static {

        final AttributeCollection.Builder builder = AttributeCollection.builder(SQL92DBSQLDialect.ATTRIBUTES);

        ATTRIBUTE_CAPABILITY_DEFINE_CREATE_TABLE_CONSTRAINT_NAME = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_DEFINE_CREATE_TABLE_CONSTRAINT_NAME, false);

        ATTRIBUTE_CAPABILITY_EXPRESSION_CASE = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_EXPRESSION_CASE, true);

        ATTRIBUTE_CAPABILITY_DROP_TABLE = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_DROP_TABLE, true);

        ATTRIBUTE_CAPABILITY_TABLE_REFERENCE_DERIVED_TABLE = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_TABLE_REFERENCE_DERIVED_TABLE, true);

        ATTRIBUTE_CAPABILITY_INSERT_AS_SELECT = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_INSERT_AS_SELECT, true);

        ATTRIBUTE_SYNTAX_TABLE_REFERENCE_KEYWORD = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_TABLE_REFERENCE_KEYWORD, "");

        ATTRIBUTE_SYNTAX_CREATE_TABLE_IF_NOT_EXISTS = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_CREATE_TABLE_IF_NOT_EXISTS, "IF NOT EXISTS");

        ATTRIBUTE_SYNTAX_DELIMITER_IDENTIFIER_CLOSING = builder.add(Accessibility.EDITABLE,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_DELIMITER_IDENTIFIER_CLOSING, "`");

        ATTRIBUTE_SYNTAX_DELIMITER_IDENTIFIER_OPENING = builder.add(Accessibility.EDITABLE,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_DELIMITER_IDENTIFIER_OPENING, "`");

        ATTRIBUTE_SYNTAX_CREATE_TABLE_TEMPORARY = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_CREATE_TABLE_TEMPORARY, "TEMPORARY");

        ATTRIBUTES = builder.build();
    }

    /**
     * The {@linkplain #getId() ID} of the {@link HiveSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getId()
     * @see HiveSQLDialect.Factory#getId()
     */
    @SuppressWarnings("hiding")
    public static final String ID = "hive";

    /**
     * The {@linkplain #getDescription() description} of the {@link HiveDBSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getDescription()
     * @see HiveDBSQLDialect.Factory#getDescription()
     */
    static final String DESCRIPTION = "Hive";

    /**
     * The {@linkplain #getName() name} of the {@link HiveDBSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getName()
     * @see HiveDBSQLDialect.Factory#getName()
     */
    static final String NAME = "Hive";


    /**
     * Constructs an {@link HiveSQLDialect} object.
     *
     * @param factory the factory that produces the instance.
     * @param dialectParameters the dialect-specific parameters controlling statement creation.
     */
    protected HiveSQLDialect(final DBSQLDialectFactory factory, final DBSQLDialectParameters dialectParameters) {
        super(factory, dialectParameters);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUnqualifiedColumnName(final String columnName) {
        Objects.requireNonNull(columnName, "columnName");
        if (columnName.isEmpty()) {
            return columnName;
        }
        return columnName.replaceFirst("^[^\\.]*\\.", "");
    }

    /**
     * {@inheritDoc} Throws an error if {@code NOT NULL} option is set.
     *
     * @throws UnsupportedOperationException {@code NOT NULL} options is used
     */
    @Override
    protected void appendColumnDefinitions(final StringBuilder statement, final CreateTableParameters parameters) {
        // validate columns
        for (final DBColumn column : parameters.getColumns()) {
            if (column.isNotNull()) {
                throw new UnsupportedOperationException(getName() + " does not support NOT NULL option");
            }
        }

        super.appendColumnDefinitions(statement, parameters);
    }

    @Override
    public SQLCommand[] getCreateTableAsSelectStatement(final DBSchemaObject schemaObject, final SQLQuery sql) {
        return new SQLCommand[] {new SQLCommand("CREATE TABLE " + createFullName(schemaObject) + " AS " + sql.getQuery())};
    }

    @Override
    public SQLCommand createInsertAsSelectStatement(final DBSchemaObject schemaObject, final SQLQuery query,
        final String... columnNames) {
        final String insert = String.format("INSERT INTO  TABLE %s %s", createFullName(schemaObject), query.getQuery());
        return new SQLCommand(insert);
    }

    @Override
    public SQLCommand getDropTableStatement(final DBSchemaObject schemaObject, final boolean cascade) {
        return super.getDropTableStatement(schemaObject, false);
    }

    @Override
    public SQLQuery forMetadataOnly(final SQLQuery sql) {
        return createLimitQuery(sql, 0);
    }

    @Override
    public SQLQuery createLimitQuery(final SQLQuery query, final long count) {
        return new SQLQuery(selectAll().getPart() + "FROM (" + query.getQuery() + "\n) " + getTempTableName()
        + " LIMIT " + count);
    }

    @Override
    public String asTable(final String expression, final String correlationName) {
        if (StringUtils.isBlank(Objects.requireNonNull(expression, "expression"))) {
            throw new IllegalArgumentException(
                "expression must not be " + (expression.isEmpty() ? "empty." : "whitespace only."));
        }
        if (StringUtils.isBlank(Objects.requireNonNull(correlationName, "correlationName"))) {
            throw new IllegalArgumentException(
                "correlationName must not be " + (correlationName.isEmpty() ? "empty." : "whitespace only."));
        }
        return expression + " " + correlationName;
    }
}
