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
 *   16.04.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.impala;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.util.LinkedHashMap;
import java.util.Map;

import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.database.datatype.mapping.AbstractDBDataTypeMappingService;
import org.knime.database.datatype.mapping.DBDestination;
import org.knime.database.datatype.mapping.DBSource;

/**
 * Impala specific data type mappings.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class ImpalaTypeMappingService extends AbstractDBDataTypeMappingService<DBSource, DBDestination> {

    private static final ImpalaTypeMappingService INSTANCE = new ImpalaTypeMappingService();

    /**
     * Gets the singleton {@link ImpalaTypeMappingService} instance.
     *
     * @return the only {@link ImpalaTypeMappingService} instance.
     */
    public static ImpalaTypeMappingService getInstance() {
        return INSTANCE;
    }

    private ImpalaTypeMappingService() {
        super(DBSource.class, DBDestination.class);

        // Default consumption paths
        final Map<DataType, SQLType> defaultConsumptionMap = new LinkedHashMap<>(getDefaultConsumptionMap());
        defaultConsumptionMap.put(BooleanCell.TYPE, JDBCType.BOOLEAN);
        defaultConsumptionMap.put(ZonedDateTimeCellFactory.TYPE, JDBCType.VARCHAR); // not supported in Impala
        setDefaultConsumptionPathsFrom(defaultConsumptionMap);

        // Default production paths
        setDefaultProductionPathsFrom(getDefaultProductionMap());

        // SQL type to database column type mapping
        addColumnType(JDBCType.DATE, "timestamp");
        addColumnType(JDBCType.INTEGER, "int");
        addColumnType(JDBCType.TIME, "timestamp");
        addColumnType(JDBCType.TIME_WITH_TIMEZONE, "timestamp");
        addColumnType(JDBCType.VARCHAR, "string");
    }
}
