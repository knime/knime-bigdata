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
 *   11.06.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.hive.node.loader;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Objects;

import org.apache.orc.TypeDescription;

/**
 * Utility class that translates Hive type Strings to JDBCTypes
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class HiveTypeUtil {

    private final static HashMap<String, Integer> m_HiveTypetoJDBCMap = createMap();

    private static HashMap<String, Integer> createMap() {
        HashMap<String, Integer> typeMap = new HashMap<String, Integer>();
        typeMap.put("VOID", java.sql.Types.NULL);
        typeMap.put("BOOLEAN", java.sql.Types.BOOLEAN);
        typeMap.put("TINYINT", java.sql.Types.TINYINT);
        typeMap.put("SMALLINT", java.sql.Types.SMALLINT);
        typeMap.put("INT", java.sql.Types.INTEGER);
        typeMap.put("BIGINT", java.sql.Types.BIGINT);
        typeMap.put("FLOAT", java.sql.Types.FLOAT);
        typeMap.put("DOUBLE", java.sql.Types.DOUBLE);
        typeMap.put("STRING", java.sql.Types.VARCHAR);
        typeMap.put("CHAR", java.sql.Types.CHAR);
        typeMap.put("VARCHAR", java.sql.Types.VARCHAR);
        typeMap.put("DATE", java.sql.Types.DATE);
        typeMap.put("TIMESTAMP", java.sql.Types.TIMESTAMP);
        typeMap.put("INTERVAL_YEAR_MONTH", java.sql.Types.OTHER);
        typeMap.put("INTERVAL_DAY_TIME", java.sql.Types.OTHER);
        typeMap.put("BINARY", java.sql.Types.BINARY);
        typeMap.put("DECIMAL", java.sql.Types.DECIMAL);
        typeMap.put("ARRAY", java.sql.Types.ARRAY);
        typeMap.put("MAP", java.sql.Types.JAVA_OBJECT);
        typeMap.put("STRUCT", java.sql.Types.STRUCT);
        typeMap.put("UNIONTYPE", java.sql.Types.OTHER);
        typeMap.put("USER_DEFINED", java.sql.Types.OTHER);

        return typeMap;
    }

    private final static HashMap<JDBCType, TypeDescription> m_ORCtoJDBCMap = createORCMap();

    private static HashMap<JDBCType, TypeDescription> createORCMap() {
        HashMap<JDBCType, TypeDescription> typeMap = new HashMap<JDBCType, TypeDescription>();
        //NUMERIC
        typeMap.put(JDBCType.TINYINT, TypeDescription.createByte());
        typeMap.put(JDBCType.SMALLINT, TypeDescription.createShort());
        typeMap.put(JDBCType.INTEGER, TypeDescription.createInt());
        typeMap.put(JDBCType.BIGINT, TypeDescription.createLong());
        typeMap.put(JDBCType.FLOAT, TypeDescription.createFloat());
        typeMap.put(JDBCType.DOUBLE, TypeDescription.createDouble());
        typeMap.put(JDBCType.DECIMAL, TypeDescription.createDecimal());

        //Date/time
        typeMap.put(JDBCType.TIMESTAMP, TypeDescription.createTimestamp());
        typeMap.put(JDBCType.DATE, TypeDescription.createDate());

        //STRING
        typeMap.put(JDBCType.VARCHAR, TypeDescription.createVarchar());
        typeMap.put(JDBCType.CHAR, TypeDescription.createChar());

        //MISC
        typeMap.put(JDBCType.BOOLEAN, TypeDescription.createBoolean());
        typeMap.put(JDBCType.BINARY, TypeDescription.createBinary());
        return typeMap;
    }

    /**
     * Converts a Hive type String into the JDBCType equivalent
     *
     * @param hiveType the String to convert
     * @return The corresponding JDBCType
     *
     */
    public static JDBCType hivetoJDBCType(final String hiveType) {
        Objects.requireNonNull(hiveType, "hiveType");
        Integer typeInt = m_HiveTypetoJDBCMap.get(hiveType.toUpperCase());
        return JDBCType.valueOf(typeInt);
    }

    /**
     * Converts a Hive type String into the JDBCType equivalent
     *
     * @param hiveType the String to convert
     * @return The corresponding JDBCType
     *
     */
    public static TypeDescription hivetoOrcType(final String hiveType) {
        return m_ORCtoJDBCMap.get(hivetoJDBCType(hiveType));
    }

}
