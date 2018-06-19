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
 *   Apr 18, 2018 (Mark Ortmann, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.spark.core.livy.node.create.ui;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Rectangle;
import java.awt.event.MouseEvent;

import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumnModel;

import org.knime.core.node.InvalidSettingsException;

/**
 * A {@link JTable} with two columns (Key and Value) that stores its data in a {@link SettingsModelKeyValue}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class KeyValueTable extends JTable {

    /** Default uid. */
    private static final long serialVersionUID = 1L;

    /** The default viewport width. */
    private static final int DEFAULT_VIEWPORT_WIDTH = 300;

    /** The default viewport height. */
    private static final int DEFAULT_VIEWPORT_HEIGHT = 300;

    /** The propert input color. */
    private static final Color PROPER_INPUT_COLOR = new Color(255, 255, 255);

    /** The wrong input color. */
    private static final Color WRONG_INPUT_COLOR = new Color(255, 120, 120);

    /**
     * Constructor.
     *
     * @param dm the underlying KeyValueModel model.
     */
    KeyValueTable(final KeyValueTableModel dm) {
        super(dm);
        setFillsViewportHeight(false);
        getTableHeader().setReorderingAllowed(false);
        setCellEditor(getColumnModel());
        setRenderer(getColumnModel());
        //        final RowSorter<KeyValueTableModel> rowSorter = new TableRowSorter<KeyValueTableModel>(dm);
        //        rowSorter.setSortKeys(Collections.singletonList(new RowSorter.SortKey(0, SortOrder.ASCENDING)));
        //        setRowSorter(rowSorter);
        setPreferredScrollableViewportSize(new Dimension(DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_HEIGHT));
        dm.fireTableDataChanged();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueTableModel getModel() {
        return (KeyValueTableModel)super.getModel();
    }

    /**
     * Sets the {@link KeyValueCellEditor} to the KEY_IDX column.
     *
     * @param model the {@link KeyValueTableModel}.
     */
    private void setCellEditor(final TableColumnModel model) {
        model.getColumn(KeyValueTableModel.KEY_IDX)
            .setCellEditor(new KeyValueCellEditor(getModel().getSettingsModel()));

    }

    /**
     * Sets the validation renders to the available columns.
     *
     * @param model the {@link KeyValueTableModel}.
     */
    private void setRenderer(final TableColumnModel model) {
        for (int i = 0; i < getColumnCount(); i++) {
            model.getColumn(i).setCellRenderer(new ValidationRenderer());
        }
    }

    /**
     * Adds an row to the table.
     *
     * @return <code>True</code> if the row was added to the table and <code>False</code> otherwise
     */
    boolean addRow() {
        // add a row to the model
        if (getModel().addRow()) {
            // highlight the newly create row
            highlightRow(getModel().getRowCount() - 1);
            return true;
        }
        return false;
    }

    /**
     * Adds all available rows to the table.
     *
     * @return <code>True</code> if the all rows were added to the table and <code>False</code> otherwise
     */
    boolean addAllRows() {
        return getModel().addAllRows();
    }

    /**
     * Removes all rows from the table.
     */
    void removeAllRows() {
        getModel().removeAllRows();
    }

    /**
     * Returns a flag indicating whether there are more rows available to be added to the table or not.
     *
     * @return <code>True</code> if there are more rows available and <code>False</code> otherwise
     */
    boolean hasMoreRows() {
        return getModel().hasMoreRows();
    }

    /**
     * Returns <code>true</code> if this table contains no elements.
     *
     * @return <code>true</code> if this table contains no elements
     */
    boolean isEmpty() {
        return getModel().isEmpty();
    }

    /**
     * Returns the component of this table.
     *
     * @return the table itself
     */
    Component getComponent() {
        return this;
    }

    /**
     * Removes all rows that are selected from the table.
     */
    void removeOnSelection() {
        // get the selected rows
        final int[] selectedRows = getSelectedRows();

        // remove them from the model
        getModel().removeRows(selectedRows);

        // update the selected rows
        if (selectedRows.length == 1) {
            if (!isEmpty()) {
                final int idx = selectedRows[0];
                highlightRow(Math.max(0, Math.min(getRowCount(), idx - 1)));
            } else {
                getSelectionModel().clearSelection();
            }
        } else {
            highlightRow(getRowCount() - 1);
        }
    }

    /**
     * Stops the editing.
     *
     * @throws InvalidSettingsException - If something went wrong when trying to call stop editing on the cell editor
     */
    void stopEditing() throws InvalidSettingsException {
        if (isEditing() && getCellEditor() != null) {
            final boolean success = getCellEditor().stopCellEditing();
            if (!success) {
                throw new InvalidSettingsException("Some settings are invalid. Please check it again.");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getToolTipText(final MouseEvent event) {
        final java.awt.Point p = event.getPoint();
        final int rowIndex = convertRowIndexToModel(rowAtPoint(p));
        final int columnIndex = convertColumnIndexToModel(columnAtPoint(p));
        return getModel().getToolTipText(rowIndex, columnIndex);
    }

    /**
     * Highlights the row on the given index.
     *
     * @param index index of the row to highlight
     */
    private void highlightRow(final int index) {
        if (index > -1) {
            final ListSelectionModel selModel = getSelectionModel();
            if (selModel != null) {
                // select the fresh added rows
                selModel.setSelectionInterval(index, index);
                // scroll first selected row into view
                scrollRectToVisible(new Rectangle(getCellRect(index, 0, true)));
            }
        }
    }

    /**
     * Cell renderer used to give visual feedback on the validity of the entries.
     *
     * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
     *
     */
    private class ValidationRenderer extends DefaultTableCellRenderer {

        /** Default uid. */
        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public Component getTableCellRendererComponent(final JTable table, final Object value, final boolean isSelected,
            final boolean hasFocus, final int row, final int column) {
            // reset the background color
            setBackground(PROPER_INPUT_COLOR);
            super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            // if the entry is not valid change the background
            try {
                getModel().validate(value, convertRowIndexToModel(row), convertColumnIndexToModel(column));
            } catch (final Exception e) {
                setBackground(WRONG_INPUT_COLOR);
            }
            return this;
        }
    }
}
