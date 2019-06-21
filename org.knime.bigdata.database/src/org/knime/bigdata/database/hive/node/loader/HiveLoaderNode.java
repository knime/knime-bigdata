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
 *   07.05.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.hive.node.loader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.orc.TypeDescription;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.database.loader.BigDataLoaderNode;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCTypeMappingService;
import org.knime.bigdata.fileformats.orc.writer.OrcKNIMEWriter;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.database.dialect.DBColumn;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;

/**
 * Implementation of the loader node for the Hive database.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class HiveLoaderNode extends BigDataLoaderNode {

    @Override
    protected AbstractFileFormatWriter createWriter(final RemoteFile<Connection> file, final DataTableSpec spec, final DBColumn[] inputColumns) throws IOException {
        return new OrcKNIMEWriter(file, spec, 100, "NONE", getORCTypesMapping(spec, inputColumns));
    }

    private static DataTypeMappingConfiguration<TypeDescription> getORCTypesMapping(final DataTableSpec spec,
        final DBColumn[] inputColumns) {

        List<TypeDescription> orcTypes = new ArrayList<>();
        for (DBColumn dbCol : inputColumns) {
            String type = dbCol.getType();
            orcTypes.add(HiveTypeUtil.hivetoOrcType(type));
        }

        final DataTypeMappingConfiguration<TypeDescription> configuration =
            ORCTypeMappingService.getInstance().createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL);

        for (int i = 0; i < spec.getNumColumns(); i++) {
            DataColumnSpec knimeCol = spec.getColumnSpec(i);
            DataType dataType = knimeCol.getType();
            TypeDescription orcType = orcTypes.get(i);
            Collection<ConsumptionPath> consumPaths =
                ORCTypeMappingService.getInstance().getConsumptionPathsFor(dataType);
            boolean found = false;
            for (ConsumptionPath path : consumPaths) {

                if (path.getConsumerFactory().getDestinationType().equals(orcType)) {
                    found = true;

                    configuration.addRule(dataType, path);
                    break;
                }
            }
            if (!found) {
                String error = String.format("Could not find ConsumptionPath for %s to JDBC Type %s via ORC Type %s",
                    dataType, inputColumns[i].getType(), orcType);
                LOGGER.error(error);
                throw new RuntimeException(error);
            }
        }

        return configuration;

    }
}
