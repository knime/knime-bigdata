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

import java.awt.Component;
import java.util.Set;

import javax.swing.DefaultCellEditor;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JTable;
import javax.swing.ListCellRenderer;

import org.apache.commons.lang3.text.WordUtils;

/**
 * Cell editor showing all available keys.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 *
 */
final class KeyValueCellEditor extends DefaultCellEditor {

    private static final long serialVersionUID = 1L;

    final SettingsModelKeyValue m_settingsModel;

    /**
     * Constructor.
     *
     * @param settingsModel The underlying settings model that supplies the keys.
     */
    KeyValueCellEditor(final SettingsModelKeyValue settingsModel) {
        super(createEditableComboBox());
        m_settingsModel = settingsModel;
        getComboBox().setRenderer(new KeyValueListCellRenderer());
    }

    private static JComboBox<String> createEditableComboBox() {
        final JComboBox<String> comboBox = new JComboBox<>();
        comboBox.setEditable(true);
        return comboBox;
    }

    /**
     * Returns the proper combobox class.
     *
     * @return the combobox
     */
    @SuppressWarnings("unchecked")
    private JComboBox<String> getComboBox() {
        return (JComboBox<String>)getComponent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Component getTableCellEditorComponent(final JTable table, final Object value, final boolean isSelected,
        final int row, final int column) {
        // get the combobox
        final JComboBox<String> box = getComboBox();

        // clear all items
        box.removeAllItems();

        final Set<String> keysToDisplay = m_settingsModel.getUnassignedKeys();
        // make sure the currently shown value is also available
        keysToDisplay.add((String)value);

        // add all available items plus the currently shown value
        for (final String key : keysToDisplay) {
            box.addItem(key);
        }

        return super.getTableCellEditorComponent(table, value, isSelected, row, column);
    }

    @Override
    public Object getCellEditorValue() {
        // get the value from the combo box editor, otherwise current value will be lost after pressing "tab"
        return getComboBox().getEditor().getItem();
    }

    /**
     * List cell renderer showing tooltips for each of the keys.
     *
     * @author Bjoern Lohrmann, KNIME GmbH
     */
    private class KeyValueListCellRenderer extends JLabel implements ListCellRenderer<String> {

        /** Default uid. */
        private static final long serialVersionUID = 1L;

        /**
         * Constructor.
         */
        private KeyValueListCellRenderer() {
            setOpaque(true);
            setVerticalAlignment(CENTER);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Component getListCellRendererComponent(final JList<? extends String> list, final String value,
            final int index, final boolean isSelected, final boolean cellHasFocus) {
            // adapt background
            if (isSelected) {
                setBackground(list.getSelectionBackground());
                setForeground(list.getSelectionForeground());
            } else {
                setBackground(list.getBackground());
                setForeground(list.getForeground());
            }
            // set the new value
            setText(value);
            // set the tooltip text
            final KeyDescriptor keyDescriptor = m_settingsModel.getSupportedKey(value);
            if (keyDescriptor != null && keyDescriptor.getDescription() != null) {
                setToolTipText(String.format("<html>%s</html>", WordUtils.wrap(keyDescriptor.getDescription(), 100, "<br/>", false)));
            }

            return this;
        }
    }
}
