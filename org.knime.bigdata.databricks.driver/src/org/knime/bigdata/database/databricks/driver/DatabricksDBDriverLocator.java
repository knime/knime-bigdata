/*
 * ------------------------------------------------------------------------
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
 * -------------------------------------------------------------------
 *
 */
package org.knime.bigdata.database.databricks.driver;

import static org.knime.database.connection.DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_INITIAL_PARAMETER_SEPARATOR;
import static org.knime.database.connection.DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_PARAMETER_SEPARATOR;
import static org.knime.database.connection.DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_PARAMETER_TO_URL;
import static org.knime.database.connection.DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_USER_AND_PASSWORD_TO_URL;
import static org.knime.database.connection.DBConnectionManagerAttributes.ATTRIBUTE_TRANSACTION_ENABLED;

import java.util.Collection;
import java.util.Collections;

import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.databricks.DatabricksPlugin;
import org.knime.database.DBType;
import org.knime.database.attribute.Attribute;
import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.driver.AbstractDriverLocator;
import org.knime.database.driver.DBDriverLocator;
import org.knime.database.util.DerivableProperties;
import org.knime.database.util.DerivableProperties.ValueType;

/**
 * A driver locator class for the official Databricks JDBC Driver.
 */
public class DatabricksDBDriverLocator extends AbstractDriverLocator {

    private static final String VERSION = "2.6.34";

    /** Driver id. */
    public static final String DRIVER_ID = "Databricks";

    /**
     * The {@link AttributeCollection} {@linkplain #getAttributes() of} Hive drivers.
     */
    public static final AttributeCollection ATTRIBUTES;

    /**
     * Attribute that contains the JDBC properties.
     */
    public static final Attribute<DerivableProperties> ATTRIBUTE_JDBC_PROPERTIES;

    static {
    	final AttributeCollection.Builder builder =
                AttributeCollection.builder(DBConnectionManagerAttributes.getAttributes());
        //add the user agent entry to default JDBC parameters
        final DerivableProperties jdbcProperties = new DerivableProperties();
        //Used to set the user agent entry for all drivers shipped by us
        jdbcProperties.setDerivableProperty("UserAgentEntry", ValueType.LITERAL, DatabricksPlugin.getUserAgent());
        ATTRIBUTE_JDBC_PROPERTIES = builder.add(Accessibility.EDITABLE,
            DBConnectionManagerAttributes.ATTRIBUTE_JDBC_PROPERTIES, jdbcProperties);
        //change only visibility but keep the default values
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_APPEND_JDBC_PARAMETER_TO_URL);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_APPEND_JDBC_INITIAL_PARAMETER_SEPARATOR);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_APPEND_JDBC_PARAMETER_SEPARATOR);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_APPEND_JDBC_USER_AND_PASSWORD_TO_URL);
        builder.add(Accessibility.HIDDEN, ATTRIBUTE_TRANSACTION_ENABLED, false);
        ATTRIBUTES = builder.build();
    }

    /**
     * Constructor for {@link DatabricksDBDriverLocator}.
     */
    public DatabricksDBDriverLocator() {
        super(ATTRIBUTES);
    }

    @Override
    public String getDriverId() {
        return DBDriverLocator.createDriverId(getDBType(), VERSION);
    }

    @Override
    public String getDriverName() {
        return "Driver for Databricks v. " + VERSION;
    }

    @Override
    public String getDriverClassName() {
        return "com.databricks.client.jdbc.Driver";
    }

    @Override
    public DBType getDBType() {
        return Databricks.DB_TYPE;
    }

    @Override
    public String getURLTemplate() {
        return "jdbc:databricks://<host>:<port>/default";
    }

    @Override
    public Collection<String> getDriverPaths() {
        return Collections.singleton("lib/" + VERSION + "/jdbc/databricks-jdbc-" + VERSION + ".jar");
    }
}
