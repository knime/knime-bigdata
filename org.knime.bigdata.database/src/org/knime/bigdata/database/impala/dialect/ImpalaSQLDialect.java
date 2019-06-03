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
package org.knime.bigdata.database.impala.dialect;

import java.util.Objects;
import java.util.regex.Pattern;

import org.knime.bigdata.database.hive.dialect.HiveSQLDialect;
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

/**
 * Dialect for Impala databases.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class ImpalaSQLDialect extends HiveSQLDialect {

    /**
     * {@link DBSQLDialectFactory} that produces {@link ImpalaSQLDialect} instances.
     *
     * @author  Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class Factory implements DBSQLDialectFactory {
        @Override
        public DBSQLDialect createDialect(final DBSQLDialectFactoryParameters parameters) {
            return new ImpalaSQLDialect(this,
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

        builder.add(Accessibility.HIDDEN, ATTRIBUTE_CAPABILITY_DEFINE_CREATE_TABLE_CONSTRAINT_NAME);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_CAPABILITY_EXPRESSION_CASE);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_CAPABILITY_DROP_TABLE);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_CAPABILITY_TABLE_REFERENCE_DERIVED_TABLE);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_CAPABILITY_INSERT_AS_SELECT);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_SYNTAX_TABLE_REFERENCE_KEYWORD);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_SYNTAX_CREATE_TABLE_IF_NOT_EXISTS);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_SYNTAX_DELIMITER_IDENTIFIER_CLOSING);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_SYNTAX_DELIMITER_IDENTIFIER_OPENING);

        // not supported by Impala:
        ATTRIBUTE_SYNTAX_CREATE_TABLE_TEMPORARY = builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_CREATE_TABLE_TEMPORARY, "");

        ATTRIBUTES = builder.build();
    }

    /**
     * The {@linkplain #getId() ID} of the {@link ImpalaSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getId()
     * @see ImpalaSQLDialect.Factory#getId()
     */
    @SuppressWarnings("hiding")
    public static final String ID = "impala";

    /**
     * The {@linkplain #getDescription() description} of the {@link ImpalaDBSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getDescription()
     * @see ImpalaDBSQLDialect.Factory#getDescription()
     */
    static final String DESCRIPTION = "Impala";

    /**
     * The {@linkplain #getName() name} of the {@link ImpalaDBSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getName()
     * @see ImpalaDBSQLDialect.Factory#getName()
     */
    static final String NAME = "Impala";


    /**
     * Constructs an {@link ImpalaSQLDialect} object.
     *
     * @param factory the factory that produces the instance.
     * @param dialectParameters the dialect-specific parameters controlling statement creation.
     */
    protected ImpalaSQLDialect(final DBSQLDialectFactory factory, final DBSQLDialectParameters dialectParameters) {
        super(factory, dialectParameters);
    }

    /**
     * {@inheritDoc} Throws an error if unsupported characters in column names detected or
     * {@code NOT NULL} option is set.
     *
     * @throws UnsupportedOperationException on invalid column names or {@code NOT NULL} options is used
     */
    @Override
    protected void appendColumnDefinitions(final StringBuilder statement, final CreateTableParameters parameters) {
        // validate columns
        for (final DBColumn column : parameters.getColumns()) {
            if (column.isNotNull()) {
                throw new UnsupportedOperationException("Impala does not support NOT NULL option");
            } else if(!Pattern.matches("^[0-9a-zA-Z_]+$", column.getName())) {
                throw new UnsupportedOperationException(
                    "Only alphabetic characters, numbers and underscore allowed in column name: " + column.getName());
            }
        }

        super.appendColumnDefinitions(statement, parameters);
    }
}
