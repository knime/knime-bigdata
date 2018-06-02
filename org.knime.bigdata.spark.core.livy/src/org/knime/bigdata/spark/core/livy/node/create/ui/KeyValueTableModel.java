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

import java.util.Arrays;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

/**
 * A Swing {@link TableModel} model implementation for {@link KeyValueTable}. The table model does actually hold the
 * data, this is all delegated to the underlying {@link SettingsModelKeyValue} instance.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 *
 */
public class KeyValueTableModel extends AbstractTableModel {

    private static final long serialVersionUID = 1L;

    /** The column index of the keys in the table. */
    public static final int KEY_IDX = 0;

    /** The column index of the values in the table. */
    public static final int VAL_IDX = 1;

    /** The column names to display */
    private static final String[] COLUMN_NAMES = new String[]{"Key", "Value"};

    /** The column classes */
    private static final Class<?>[] COLUMN_CLASSES = new Class<?>[]{String.class, String.class};

    /** The underlying settings model */
    private final SettingsModelKeyValue m_settingsModel;

    /**
     * Constructor.
     *
     * @param settingsModel the available properties
     */
    public KeyValueTableModel(final SettingsModelKeyValue settingsModel) {
        super();
        m_settingsModel = settingsModel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final String getColumnName(final int column) {
        return COLUMN_NAMES[column];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Class<?> getColumnClass(final int columnIndex) {
        return COLUMN_CLASSES[columnIndex];
    }

    /**
     * Removes the provided rows from this model.
     *
     * @param rowIndices the indices of the rows to remove
     */
    void removeRows(final int... rowIndices) {
        // sort the indices and remove them from highest to lowest
        Arrays.sort(rowIndices);
        for (int i = rowIndices.length - 1; i >= 0; i--) {
            m_settingsModel.removeKey(rowIndices[i]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCellEditable(final int rowIndex, final int columnIndex) {
        return true;
    }

    /**
     * Returns the tooltip text for the given row and column index.
     *
     * @param rowIndex the row index
     * @param columnIndex the column index
     * @return the tooltip text
     */
    String getToolTipText(final int rowIndex, final int columnIndex) {
        if (checkRanges(rowIndex, columnIndex)) {
            final String key = m_settingsModel.getKey(rowIndex);
            
            String tooltip = null;
            if (m_settingsModel.isSupported(key)) {
                tooltip = m_settingsModel.getSupportedKey(key).getDescription();
            }
            return tooltip;
        }
        return null;
    }

    /**
     * Adds a row to this model.
     *
     * @return <code>True</code> if a new row has been added to this model and <code>False</code> otherwise
     */
    boolean addRow() {
        // check if there are more rows available
        if (m_settingsModel.hasNext()) {
            m_settingsModel.addNext();
            return true;
        }
        return false;
    }

    /**
     * Adds all available rows to this model.
     *
     * @return <code>True</code> if new rows have been added to this model and <code>False</code> otherwise
     *
     */
    boolean addAllRows() {
        // check if there are more rows available
        if (m_settingsModel.hasNext()) {
            m_settingsModel.addAll();
            return true;
        }
        return false;
    }

    /**
     * Removes all rows from this model.
     */
    void removeAllRows() {
        m_settingsModel.clear();
    }

    /**
     * Returns <code>true</code> if this model contains more rows.
     *
     * @return <code>true</code> if this model contains more rows
     */
    boolean hasMoreRows() {
        return m_settingsModel.hasNext();
    }

    /**
     * Returns <code>true</code> if this model contains no elements.
     *
     * @return <code>true</code> if this model contains no elements
     */
    boolean isEmpty() {
        return m_settingsModel.getKeyValuePairs().isEmpty();
    }

    /**
     * Returns the number of rows stored by this model.
     */
    @Override
    public int getRowCount() {
        return m_settingsModel.getKeyValuePairs().size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setValueAt(final Object aValue, final int rowIndex, final int columnIndex) {
        // check the ranges
        if (checkRanges(rowIndex, columnIndex)) {
            // get the row
            final String newVal = (String)aValue;
            final String prevKeyAtRow = m_settingsModel.getKey(rowIndex);

            switch (columnIndex) {
                case KEY_IDX:
                    // only update if the value changed
                    if (!newVal.equals(prevKeyAtRow)) {
                        // a new key also refers to a new value
                        m_settingsModel.replaceKeyValuePair(prevKeyAtRow, newVal,
                            m_settingsModel.getSupportedKey(newVal).getDefaultValue());
                    }
                    break;
                case VAL_IDX:
                    final String prevVal = m_settingsModel.getValue(prevKeyAtRow);
                    if (!newVal.equals(prevVal)) {
                        m_settingsModel.setKeyValuePair(prevKeyAtRow, newVal);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValueAt(final int rowIndex, final int columnIndex) {
        if (checkRanges(rowIndex, columnIndex)) {
            final String key = m_settingsModel.getKey(rowIndex);
            if (columnIndex == KEY_IDX) {
                return key;
            } else {
                return m_settingsModel.getValue(key);
            }
        }
        return null;
    }

    /**
     * Checks whether the provided row and column index are valid.
     *
     * @param rowIndex the row index
     * @param columnIndex the column index
     * @return <code>True</code> if the indices lie within the range
     */
    private boolean checkRanges(final int rowIndex, final int columnIndex) {
        return (rowIndex >= 0 && rowIndex < getRowCount() && columnIndex >= 0 && columnIndex < getColumnCount());
    }

    SettingsModelKeyValue getSettingsModel() {
        return m_settingsModel;
    }

    /**
     * Validates the correctness of the value.
     *
     * @param aValue the value to be validated
     * @param rowIndex the row index
     * @param columnIndex the column index
     * @throws Exception - If the value is not valid
     */
    void validate(final Object aValue, final int rowIndex, final int columnIndex) throws Exception {
        if (checkRanges(rowIndex, columnIndex)) {
            final String newVal = (String)aValue;
            switch (columnIndex) {
                case KEY_IDX:
                    // check if the value is among the allowed rows
                    if (m_settingsModel.getSupportedKey(newVal) == null) {
                        throw new UnsupportedDataTypeException("The key: " + newVal + " is not supported anymore");
                    }
                    break;
                case VAL_IDX:
                    // check if the value fulfills the ConfigDef requirements
                    final String key = m_settingsModel.getKey(rowIndex);
                    m_settingsModel.getSupportedKey(key).validateValue(newVal);
                    break;
                default:
                    break;
            }
        }
    }
}
