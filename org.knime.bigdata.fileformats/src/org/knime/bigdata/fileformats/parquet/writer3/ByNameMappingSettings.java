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
 *   Jan 28, 2026 (paulbaernreuther): created
 */
package org.knime.bigdata.fileformats.parquet.writer3;

import static org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.getIdForConsumptionPath;

import org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.FilterType;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;

@SuppressWarnings("restriction")
final class ByNameMappingSettings implements NodeParameters {
    ByNameMappingSettings() {
        //default constructor for deserialization
    }

    ByNameMappingSettings(final FilterType filterType, final String columnName, final DataType knimeType,
        final ConsumptionPath consumptionPath) {
        this(filterType, columnName, knimeType, getIdForConsumptionPath(consumptionPath));
    }

    ByNameMappingSettings(final FilterType filterType, final String fromColName, final DataType fromColType,
        final String toColType) {
        this.m_filterType = filterType;
        this.m_fromColName = fromColName;
        this.m_fromColType = fromColType;
        this.m_toColType = toColType;
    }

    @Widget(title = "Column selection type", description = "The option allows you to select how the column is matched.")
    @ValueSwitchWidget
    FilterType m_filterType = FilterType.MANUAL;

    @Widget(title = "Column name", description = "The column name or regex expression.")
    @ValueReference(FromColRef.class)
    String m_fromColName = "";

    @Widget(title = "KNIME type", description = "KNIME data type to map from.")
    @ValueReference(FromColTypeRef.class)
    @Modification.WidgetReference(FromColTypeRef.class)
    DataType m_fromColType;

    @Widget(title = "Mapping to", description = "Parquet data type to map to.")
    @Modification.WidgetReference(ToColTypeRef.class)
    String m_toColType;

    interface FromColRef extends ParameterReference<String> {
    }

    interface FromColTypeRef extends ParameterReference<DataType>, Modification.Reference {
    }

    interface ToColTypeRef extends Modification.Reference {
    }

}