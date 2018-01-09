/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
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
 * -------------------------------------------------------------------
 *
 */
package org.knime.bigdata.spark.node.preproc.groupby.dialog.column;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumnModel;

import org.knime.base.data.aggregation.ColumnAggregator;
import org.knime.base.data.aggregation.dialogutil.AbstractAggregationPanel;
import org.knime.base.data.aggregation.dialogutil.AggregationFunctionAndRowTableCellRenderer;
import org.knime.base.data.aggregation.dialogutil.AggregationFunctionRowTableCellRenderer;
import org.knime.base.data.aggregation.dialogutil.AggregationFunctionRowTableCellRenderer.ValueRenderer;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionCombinationProvider;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.aggregation.AggregationFunctionProvider;
import org.knime.core.node.util.DataColumnSpecListCellRenderer;


/**
 * This panel allows the user to define aggregations per column.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @author Tobias Koetter, KNIME GmbH
 */
public class ColumnAggregationFunctionPanel extends AbstractAggregationPanel<ColumnAggregationFunctionTableModel,
ColumnAggregationFunctionRow, DataColumnSpec> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnAggregationFunctionPanel.class);

    /**This field holds all columns of the input table.*/
    private final List<DataColumnSpec> m_avAggrColSpecs = new LinkedList<>();

    private ColumnAggregationFunctionRowTableCellEditor m_aggregationFunctionCellEditor;

    /**
     * {@inheritDoc}
     */
    @Override
    protected JPopupMenu createTablePopupMenu() {
        final JPopupMenu menu = new JPopupMenu();
        final JMenuItem invalidRowsMenu = createInvalidRowsSelectionMenu();
        if (invalidRowsMenu != null) {
            menu.add(invalidRowsMenu);
            menu.addSeparator();
        }
        createColumnSelectionMenu(menu);
        menu.addSeparator();
        createAggregationSection(menu);
        return menu;
    }

    /**
     * Adds the column selection section to the given menu.
     * This section allows the user to select all rows that are compatible
     * to the chosen data type at once.
     * @param menu the menu to append the column selection section
     */
    private void createColumnSelectionMenu(final JPopupMenu menu) {
        final Collection<DataType> existingTypes = getAllPresentTypes();
        if (existingTypes.size() < 3) {
            //create no sub menu if their are to few different types
            for (final DataType type : existingTypes) {
                if (type == DoubleCell.TYPE || type == StringCell.TYPE) {
                    //skip the general and numerical types
                    continue;
                }
                final JMenuItem selectCompatible =
                    new JMenuItem("Select " + type.toString() + " columns");
                selectCompatible.addActionListener(new ActionListener() {
                    /**
                     * {@inheritDoc}
                     */
                    @Override
                    public void actionPerformed(final ActionEvent e) {
                        selectCompatibleRows(type);
                    }
                });
                menu.add(selectCompatible);
            }
        } else {
            //create a column selection sub menu
            final JMenu menuItem = new JMenu("Select all...");
            final JMenuItem supportedMenu = menu.add(menuItem);
            for (final DataType type : existingTypes) {
                if (type == DoubleCell.TYPE) {
                    //skip the general and numerical types
                    continue;
                }
                final JMenuItem selectCompatible =
                    new JMenuItem(type.toString() + " columns");
                selectCompatible.addActionListener(new ActionListener() {
                    /**
                     * {@inheritDoc}
                     */
                    @Override
                    public void actionPerformed(final ActionEvent e) {
                        selectCompatibleRows(type);
                    }
                });
                supportedMenu.add(selectCompatible);
            }
        }
      //add the select numerical columns entry if they are available
        final Collection<Integer> numericIdxs =
            getTableModel().getCompatibleRowIdxs(DoubleCell.TYPE);
        if (numericIdxs != null && !numericIdxs.isEmpty()) {
            final JMenuItem selectNoneNumerical = new JMenuItem("Select all numerical columns");
            selectNoneNumerical.addActionListener(new ActionListener() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public void actionPerformed(final ActionEvent e) {
                    updateSelection(numericIdxs);
                }
            });
            menu.add(selectNoneNumerical);
        }
        //add the select none numerical columns entry if they are available
        final Collection<Integer> nonNumericIdxs =
            getTableModel().getNotCompatibleRowIdxs(DoubleCell.TYPE);
        if (nonNumericIdxs != null && !nonNumericIdxs.isEmpty()) {
            final JMenuItem selectNoneNumerical = new JMenuItem("Select all non-numerical columns");
            selectNoneNumerical.addActionListener(new ActionListener() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public void actionPerformed(final ActionEvent e) {
                    updateSelection(nonNumericIdxs);
                }
            });
            menu.add(selectNoneNumerical);
        }
        //add the select all columns entry
        final JMenuItem selectAll = new JMenuItem("Select all columns");
        selectAll.addActionListener(new ActionListener() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void actionPerformed(final ActionEvent e) {
                getTable().selectAll();
            }
        });
        menu.add(selectAll);
    }

    /**
     * @return all {@link DataType}s with at least one row in the table
     */
    private Collection<DataType> getAllPresentTypes() {
        final List<ColumnAggregationFunctionRow> rows = getTableModel().getRows();
        final Set<DataType> types = new HashSet<>();
        for (ColumnAggregationFunctionRow row : rows) {
            types.add(row.getColumnSpec().getType());
        }
        return types;
    }

    /**
     * Adds the aggregation method section to the given menu.
     * This section allows the user to set an aggregation method for
     * all selected columns.
     * @param menu the menu to append the aggregation section
     */
    private void createAggregationSection(final JPopupMenu menu) {
        if (getSelectedRows().length <= 0) {
                final JMenuItem noneSelected = new JMenuItem("Select a column to change method");
                noneSelected.setEnabled(false);
                menu.add(noneSelected);
                return;
        }
        final List<SparkSQLAggregationFunction> methodList = getMethods4SelectedItems();
        //we need no sub menu for a single group
        for (final SparkSQLAggregationFunction method : methodList) {
            final JMenuItem methodItem = new JMenuItem(method.getLabel());
            methodItem.setToolTipText(method.getDescription());
            methodItem.addActionListener(new ActionListener() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public void actionPerformed(final ActionEvent e) {
                    changeAggregationMethod(method.getId());
                }
            });
            menu.add(methodItem);
        }
    }


    /** Constructor for class AggregationColumnPanel. */
    public ColumnAggregationFunctionPanel() {
        this(" Aggregation settings ");
    }

   /**
     * @param title the title of the border or <code>null</code> for no border
     */
    public ColumnAggregationFunctionPanel(final String title) {
       super(title, " Available columns ", new DataColumnSpecListCellRenderer(),
           " To change multiple columns use right mouse click for context menu. ",
           new ColumnAggregationFunctionTableModel());
   }

    /**
     * @param provider the {@link AggregationFunctionProvider}
     */
    public void setAggregationFunctionProvider(final AggregationFunctionProvider<SparkSQLAggregationFunction> provider) {
        getTableModel().setAggregationFunctionProvider(provider);
        m_aggregationFunctionCellEditor.setAggregationFunctionProvider(provider);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void adaptTableColumnModel(final TableColumnModel columnModel) {
        columnModel.getColumn(0).setCellRenderer(
                new AggregationFunctionRowTableCellRenderer<>(new ValueRenderer<ColumnAggregationFunctionRow>() {
                    @Override
                    public void renderComponent(final DefaultTableCellRenderer c,
                        final ColumnAggregationFunctionRow row) {
                        final DataColumnSpec spec = row.getColumnSpec();
                        c.setText(spec.getName());
                        c.setIcon(spec.getType().getIcon());
                    }
                }, true, "Double click to remove column. Right mouse click for context menu."));
        m_aggregationFunctionCellEditor = new ColumnAggregationFunctionRowTableCellEditor(null);
        columnModel.getColumn(1).setCellEditor(m_aggregationFunctionCellEditor);
        columnModel.getColumn(1).setCellRenderer(new AggregationFunctionAndRowTableCellRenderer());
        columnModel.getColumn(0).setPreferredWidth(170);
        columnModel.getColumn(1).setPreferredWidth(150);
    }

    /**
     * Changes the aggregation method of all selected rows to the method
     * with the given label.
     * @param methodId the label of the aggregation method
     */
    protected void changeAggregationMethod(final String methodId) {
        final int[] selectedRows = getSelectedRows();
        AggregationFunctionProvider<SparkSQLAggregationFunction> provider = getTableModel().getAggregationFunctionProvider();
        if (provider != null) {
            getTableModel().setAggregationFunction(selectedRows, provider.getFunction(methodId));
            final Collection<Integer> idxs = new LinkedList<>();
            for (final int i : selectedRows) {
                idxs.add(Integer.valueOf(i));
            }
            updateSelection(idxs);
        } else {
            LOGGER.error("Aggregation function provider shouldn not be null");
        }
    }

    /**
     * Selects all rows that are compatible with the given type.
     * @param type the type to check for compatibility
     */
    protected void selectCompatibleRows(final DataType type) {
        final Collection<Integer> idxs = getTableModel().getCompatibleRowIdxs(type);
        updateSelection(idxs);
    }

    /**
     * Returns the number of rows that are compatible to the given type.
     * @param type the type to check for
     * @return the number of compatible rows
     */
    int noOfCompatibleRows(final DataType type) {
        return getTableModel().getCompatibleRowIdxs(type).size();
    }

    /**
     * @param excludeColNames the name of all columns that should be
     * excluded from the aggregation panel
     */
    public void excludeColsChange(final Collection<String> excludeColNames) {
        final Set<String> excludeColNameSet = new HashSet<>(excludeColNames);
        final List<DataColumnSpec> newList = new LinkedList<>();
        //include all columns that are not in the exclude list
        for (final DataColumnSpec colSpec : m_avAggrColSpecs) {
            if (!excludeColNameSet.contains(colSpec.getName())) {
                newList.add(colSpec);
            }
        }
        final List<ColumnAggregationFunctionRow> oldAggregators = getTableModel().getRows();
        final List<ColumnAggregationFunctionRow> newAggregators = new LinkedList<>();
        for (final ColumnAggregationFunctionRow aggregator : oldAggregators) {
            if (!excludeColNameSet.contains(aggregator.getColumnSpec().getName())) {
                newAggregators.add(aggregator);
            }
        }
        initialize(newList, newAggregators, getInputTableSpec());
    }

    /**
     * @return the list of manual aggregation functions to use
     */
    public List<ColumnAggregationFunctionRow> getManualColumnAggregationFunctions() {
        return getTableModel().getRows();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validate() throws InvalidSettingsException {
        final List<ColumnAggregationFunctionRow> rows = getTableModel().getRows();
        for (final ColumnAggregationFunctionRow row : rows) {
            SparkSQLAggregationFunction function = row.getFunction();
            try {
                function.validate();
            } catch (InvalidSettingsException e) {
                throw new InvalidSettingsException("Exception for column '" + row.getColumnSpec().getName()
                    + "' and function '" + function.getLabel() + "': " + e.getMessage());
            }
            if (!row.isValid()) {
                throw new InvalidSettingsException("Row for column '" + row.getColumnSpec().getName()
                    + "' and function '" + function.getLabel() + "' is invalid.");
            }
        }

    }

    /**
     * Initializes the panel.
     *
     * @param spec the {@link DataTableSpec} of the input table
     * @param colAggrs the {@link List} of {@link ColumnAggregator}s that are initially used
     * @param functionProvider Provides the available aggregation functions.
     */
    public void initialize(final DataTableSpec spec, final List<ColumnAggregationFunctionRow> colAggrs,
        final SparkSQLFunctionCombinationProvider functionProvider) {

        setAggregationFunctionProvider(functionProvider);

        m_avAggrColSpecs.clear();
        final List<DataColumnSpec> listElements = new LinkedList<>();
        for (final DataColumnSpec colSpec : spec) {
            m_avAggrColSpecs.add(colSpec);
            listElements.add(colSpec);
        }
      //remove all invalid column aggregator
        final List<ColumnAggregationFunctionRow> colAggrs2Use = new ArrayList<>(colAggrs.size());
        for (final ColumnAggregationFunctionRow colAggr : colAggrs) {
            final DataColumnSpec colSpec = spec.getColumnSpec(colAggr.getColumnSpec().getName());
            final boolean valid;
            if (colSpec != null && colSpec.getType().equals(colAggr.getColumnSpec().getType())) {
                valid = true;
            } else {
                valid = false;
            }
            colAggr.setValid(valid);
            colAggrs2Use.add(colAggr);
        }
        initialize(listElements, colAggrs2Use, spec);
    }

    /**
     * @return a label list of all supported methods for the currently
     * selected rows
     */
    protected List<SparkSQLAggregationFunction> getMethods4SelectedItems() {
        final int[] selectedColumns = getSelectedRows();
        final Set<DataType> types = new HashSet<>(selectedColumns.length);
        for (final int row : selectedColumns) {
            final ColumnAggregationFunctionRow aggregator = getTableModel().getRow(row);
            types.add(aggregator.getColumnSpec().getType());
        }
        final DataType superType = CollectionCellFactory.getElementType(types.toArray(new DataType[0]));
        final List<SparkSQLAggregationFunction> list =
                getTableModel().getAggregationFunctionProvider().getCompatibleFunctions(superType, true);
        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ColumnAggregationFunctionRow createRow(final DataColumnSpec colSpec) {
        AggregationFunctionProvider<SparkSQLAggregationFunction> provider = getTableModel().getAggregationFunctionProvider();
        final SparkSQLAggregationFunction defaultFunction = provider.getDefaultFunction(colSpec.getType());
        return new ColumnAggregationFunctionRow(colSpec, defaultFunction);
    }
}
