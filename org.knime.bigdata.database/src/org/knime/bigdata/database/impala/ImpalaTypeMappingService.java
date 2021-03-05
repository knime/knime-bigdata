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
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.core.data.time.localtime.LocalTimeCellFactory;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.database.datatype.mapping.AbstractDBDataTypeMappingService;

/**
 * Impala specific data type mappings.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class ImpalaTypeMappingService extends AbstractDBDataTypeMappingService<ImpalaSource, ImpalaDestination> {

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
        super(ImpalaSource.class, ImpalaDestination.class);

        // Default consumption paths
        final Map<DataType, Triple<DataType, Class<?>, SQLType>> defaultConsumptionMap =
                new LinkedHashMap<>(getDefaultConsumptionTriples());
        addTriple(defaultConsumptionMap, BooleanCell.TYPE, Boolean.class, JDBCType.BOOLEAN);
        addTriple(defaultConsumptionMap, ZonedDateTimeCellFactory.TYPE, String.class, JDBCType.VARCHAR); // not supported in Impala
        addTriple(defaultConsumptionMap, LocalDateCellFactory.TYPE, LocalDate.class, JDBCType.TIMESTAMP);

        // Impala supports time columns, but parquet can't store a time as timestamp...
        addTriple(defaultConsumptionMap, LocalTimeCellFactory.TYPE, String.class, JDBCType.VARCHAR);

        setDefaultConsumptionTriples(defaultConsumptionMap);


        // Default production paths
        setDefaultProductionTriples(getDefaultProductionTriples());

        // SQL type to database column type mapping
        addColumnType(JDBCType.BIT, "boolean");
        addColumnType(JDBCType.INTEGER, "int");
        addColumnType(JDBCType.VARCHAR, "string");
    }
}
