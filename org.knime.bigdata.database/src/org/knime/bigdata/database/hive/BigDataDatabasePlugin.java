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
 *   Jun 24, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.database.hive;

import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;

import org.eclipse.core.runtime.Plugin;
import org.knime.bigdata.database.impala.ImpalaDestination;
import org.knime.bigdata.database.impala.ImpalaSource;
import org.knime.core.data.convert.map.ConsumerRegistry;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.database.datatype.mapping.DBCellValueConsumerFactory;
import org.knime.database.datatype.mapping.DBCellValueProducerFactory;
import org.knime.database.datatype.mapping.DBDestination;
import org.knime.database.datatype.mapping.DBSource;
import org.osgi.framework.BundleContext;

/**
 * KNIME BigData Database plug-in.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class BigDataDatabasePlugin extends Plugin {

    @Override
    public void start(final BundleContext context) throws Exception {
        registerImpalaConsumers();
        registerImpalaProducers();
    }

    private static void registerImpalaConsumers() {
        final ConsumerRegistry<SQLType, ImpalaDestination> reg =
            MappingFramework.forDestinationType(ImpalaDestination.class);
        reg.setParent(DBDestination.class);
        reg.unregisterAllConsumers();

        reg.register(new DBCellValueConsumerFactory<>(LocalDate.class, JDBCType.TIMESTAMP, (ps, parameters, v) -> {
            ps.setDate(parameters.getColumnIndex(), Date.valueOf(v));
        }));
    }

    private static void registerImpalaProducers() {
        final ProducerRegistry<SQLType, ImpalaSource> reg = MappingFramework.forSourceType(ImpalaSource.class);
        reg.setParent(DBSource.class);
        reg.unregisterAllProducers();

        reg.register(new DBCellValueProducerFactory<>(JDBCType.TIMESTAMP, LocalDate.class, (rs, parameters) -> {
            final Timestamp value = rs.getTimestamp(parameters.getColumnIndex());
            return value == null ? null : value.toLocalDateTime().toLocalDate();
        }));
        reg.register(new DBCellValueProducerFactory<>(JDBCType.TIMESTAMP, LocalTime.class, (rs, parameters) -> {
            // Hive Driver: getTime is not supported, getTimestamp+getString fails with parse error
            // Cloudera Driver: getTime+getTimestamp+getString works
            final Time value = rs.getTime(parameters.getColumnIndex());
            return value == null ? null : value.toLocalTime();
        }));
    }
}
