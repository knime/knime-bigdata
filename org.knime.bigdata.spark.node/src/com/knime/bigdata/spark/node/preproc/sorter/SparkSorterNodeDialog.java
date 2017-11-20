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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 * History
 *   02.02.2005 (cebron): created
 */
package com.knime.bigdata.spark.node.preproc.sorter;

import java.util.Arrays;
import java.util.List;

import javax.swing.JScrollPane;

import org.knime.base.node.preproc.sorter.SorterNodeDialogPanel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.node.MLlibNodeSettings;


/**
 * Dialog for choosing the columns that will be sorted. It is also possible to
 * set the order of columns
 *
 * @author Nicolas Cebron, University of Konstanz
 */
public class SparkSorterNodeDialog extends NodeDialogPane {
    //TK_TODO: Copied over from KNIME sorter due to access restrictions. Original version needs to be adapted to allow hiding of RowKey
    private static final NodeLogger LOGGER = NodeLogger
            .getLogger(SparkSorterNodeDialog.class);

    /**
     * The tab's name.
     */
    private static final String TAB = "Sorting Filter";

    /*
     * Hold the Panel
     */
    private final SparkSorterNodeDialogPanel2 m_panel;

    /*
     * The initial number of SortItems that the SorterNodeDialogPanel should
     * show.
     */
    private static final int NRSORTITEMS = 3;

    /**
     * Creates a new {@link NodeDialogPane} for the Sorter Node in order to
     * choose the desired columns and the sorting order (ascending/ descending).
     * @param inclRowKeys set to <code>true</code> if RowKey option should be visible
     */
    SparkSorterNodeDialog(final boolean inclRowKeys) {
        super();
        m_panel = new SparkSorterNodeDialogPanel2(inclRowKeys);
        super.addTab(TAB, new JScrollPane(m_panel));
    }

    /**
     * Calls the update method of the underlying update method of the
     * {@link SorterNodeDialogPanel} using the input data table spec from this
     * {@link SparkSorterNodeModel}.
     *
     * @param settings the node settings to read from
     * @param specs the input specs
     *
     * @see NodeDialogPane#loadSettingsFrom(NodeSettingsRO, DataTableSpec[])
     * @throws NotConfigurableException if the dialog cannot be opened.
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {
        if (specs.length == 0 || specs[0] == null) {
            throw new NotConfigurableException("No columns to sort.");
        }
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, specs);
        List<String> list = null;
        boolean[] sortOrder = null;

        if (settings.containsKey(SparkSorterNodeModel.INCLUDELIST_KEY)) {
            try {
                String[] alist =
                    settings.getStringArray(SparkSorterNodeModel.INCLUDELIST_KEY);
                if (alist != null) {
                    list = Arrays.asList(alist);
                }
            } catch (InvalidSettingsException ise) {
                LOGGER.error(ise.getMessage());
            }
        }

        if (settings.containsKey(SparkSorterNodeModel.SORTORDER_KEY)) {
            try {
                sortOrder = settings
                        .getBooleanArray(SparkSorterNodeModel.SORTORDER_KEY);
            } catch (InvalidSettingsException ise) {
                LOGGER.error(ise.getMessage());
            }
        }
        if (list != null) {
            if (list.size() == 0 || list.size() != sortOrder.length) {
                list = null;
                sortOrder = null;
            }
        }
        boolean sortMissingToEnd = settings.getBoolean(
                SparkSorterNodeModel.MISSING_TO_END_KEY, false);
        // set the values on the panel
        m_panel.update(tableSpecs[0], list, sortOrder, NRSORTITEMS, sortMissingToEnd);
    }

    /**
     * Sets the list of columns to include and the sorting order list inside the
     * underlying {@link SparkSorterNodeModel} retrieving them from the
     * {@link SorterNodeDialogPanel}.
     *
     * @param settings the node settings to write into
     * @throws InvalidSettingsException if settings are not valid
     * @see NodeDialogPane#saveSettingsTo(NodeSettingsWO)
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        assert (settings != null);
        m_panel.checkValid();
        List<String> inclList = m_panel.getIncludedColumnList();
        settings.addStringArray(SparkSorterNodeModel.INCLUDELIST_KEY, inclList
                .toArray(new String[inclList.size()]));
        settings.addBooleanArray(SparkSorterNodeModel.SORTORDER_KEY, m_panel
                .getSortOrder());
        settings.addBoolean(SparkSorterNodeModel.MISSING_TO_END_KEY,
                m_panel.isSortMissingToEnd());
    }
}
