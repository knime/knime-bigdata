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
 */
package org.knime.bigdata.databricks.sqlwarehouse;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.sql.SQLWarehouseAPI;
import org.knime.bigdata.databricks.rest.sql.SQLWarehouseInfo;
import org.knime.bigdata.databricks.rest.sql.SQLWarehouseInfoList;
import org.knime.core.data.sort.AlphanumericComparator;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Layout;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Section;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Widget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.ChoicesProvider;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.StringChoice;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.StringChoicesProvider;
import org.knime.core.webui.node.dialog.defaultdialog.widget.handler.WidgetHandlerException;

/**
 * Node settings for the Databricks SQL Warehouse Connector.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
public class SQLWarehouseConnectorSettings implements DefaultNodeSettings {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SQLWarehouseConnectorSettings.class);

    private static final Comparator<SQLWarehouseInfo> COMPARATOR =
        Comparator.comparing(i -> i.name, AlphanumericComparator.NATURAL_ORDER);

    @Section(title = "SQL Warehouse Connector")
    interface MainSection {
    }

    @Widget( //
        title = "Warehouse", //
        description = "Name of the SQL Warehouse to connect." //
    )
    @ChoicesProvider(WarehouseChoiceProvider.class)
    @Layout(MainSection.class)
    String m_warehouseId = "";

    @Override
    public void validate() throws InvalidSettingsException {
        if (StringUtils.isAllBlank(m_warehouseId)) {
            throw new InvalidSettingsException("Please select a SQL Warehouse.");
        }
    }

    private static final class WarehouseChoiceProvider implements StringChoicesProvider {

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final DefaultNodeSettingsContext context) {
            try {
                final SQLWarehouseInfoList warehouseList = DatabricksRESTClient //
                    .fromSingleWorkspaceInputPort(SQLWarehouseAPI.class, context.getPortObjectSpecs()) //
                    .listWarehouses();
                return Arrays.stream(warehouseList.warehouses) //
                    .sorted(COMPARATOR) //
                    .map(i -> new StringChoice(i.id, i.name)) //
                    .toList();
            } catch (final Exception e) { // NOSONAR catch all exceptions here
                LOGGER.info("Unable to fetch Warehouse list.", e);
                throw new WidgetHandlerException(e.getMessage());
            }
        }
    }
}
