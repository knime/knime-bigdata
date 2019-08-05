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
package org.knime.bigdata.database.hive;

import static java.util.Collections.emptySet;

import java.util.Collection;

import org.apache.hive.jdbc.HiveDriver;
import org.knime.database.DBType;
import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.driver.AbstractDriverLocator;

/**
 * This class contains the Hive driver definition. The definition will be used by Eclipse extensions API to create a
 * database driver instance.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class HiveDriverLocator extends AbstractDriverLocator {

    /**Driver id. */
    public static final String DRIVER_ID = "hive";

    /**
     * The {@link AttributeCollection} {@linkplain #getAttributes() of} Hive drivers.
     */
    public static final AttributeCollection ATTRIBUTES;


    static {
        final AttributeCollection.Builder builder =
            AttributeCollection.builder(DBConnectionManagerAttributes.getAttributes());
        builder.add(Accessibility.EDITABLE, DBConnectionManagerAttributes.ATTRIBUTE_VALIDATION_QUERY, "SELECT 1");
        builder.add(Accessibility.EDITABLE, DBConnectionManagerAttributes.ATTRIBUTE_METADATA_IN_CONFIGURE_ENABLED, false);
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_PARAMETER_TO_URL, true);
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_INITIAL_PARAMETER_SEPARATOR, ";");
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_PARAMETER_SEPARATOR, ";");
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_USER_AND_PASSWORD_TO_URL,
            false);
        ATTRIBUTES = builder.build();
    }

    /**
     * Constructs a {@link HiveDriverLocator}
     */
    public HiveDriverLocator(){
        super(ATTRIBUTES);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDriverId() {
        return DRIVER_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDriverName() {
        return "Apache Hive JDBC Driver";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDriverClassName() {
        return HiveDriver.class.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DBType getDBType() {
        return Hive.DB_TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getURLTemplate() {
        return "jdbc:hive2://<host>:<port>/[database]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> getDriverPaths() {
        return emptySet();
    }

}
