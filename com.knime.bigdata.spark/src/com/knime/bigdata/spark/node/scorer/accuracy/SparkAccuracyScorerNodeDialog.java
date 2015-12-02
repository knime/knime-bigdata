/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package com.knime.bigdata.spark.node.scorer.accuracy;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;

import org.knime.base.util.SortingOptionPanel;
import org.knime.base.util.SortingStrategy;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.DataColumnSpecListCellRenderer;

import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 * A dialog for the scorer to set the two table columns to score for.
 *
 * TODO: This is mostly copy & paste from AccuracyScorerNodeDialog. If possible, share the dialog with classic KNIME accuarcy scorer node.
 *
 * @author Christoph Sieb, University of Konstanz
 * @author Thomas Gabriel, University of Konstanz
 */
public final class SparkAccuracyScorerNodeDialog extends NodeDialogPane {
    /*
     * The text field for the first column to compare The first column
     * represents the real classes of the data
     */
    private final JComboBox<DataColumnSpec> m_firstColumns;

    /*
     * The text field for the second column to compare The second column
     * represents the predicted classes of the data
     */
    private final JComboBox<DataColumnSpec> m_secondColumns;

    /*
     * The sorting option for the values.
     */
    private final SortingOptionPanel m_sortingOptions;

    /* The check box specifying if a prefix should be added or not. */
    private JCheckBox m_flowvariableBox;

    /* The text field specifying the prefix for the flow variables. */
    private JTextField m_flowVariablePrefixTextField;

    private static final SortingStrategy[] SUPPORTED_NUMBER_SORT_STRATEGIES = new SortingStrategy[] {
        SortingStrategy.Numeric, SortingStrategy.Lexical
    };

    private static final SortingStrategy[] SUPPORTED_STRING_SORT_STRATEGIES = new SortingStrategy[] {
        SortingStrategy.Lexical
    };

    /**
     * Creates a new {@link NodeDialogPane} for scoring in order to set the two
     * columns to compare.
     */
    public SparkAccuracyScorerNodeDialog() {
        super();

        JPanel p = new JPanel();
        p.setLayout(new BoxLayout(p, BoxLayout.Y_AXIS));

        m_firstColumns = new JComboBox<DataColumnSpec>();
        m_firstColumns.setRenderer(new DataColumnSpecListCellRenderer());
        m_secondColumns = new JComboBox<DataColumnSpec>();
        m_secondColumns.setRenderer(new DataColumnSpecListCellRenderer());
        m_sortingOptions = new SortingOptionPanel();
        m_sortingOptions.setBorder(new TitledBorder("Sorting of values in tables"));

        JPanel firstColumnPanel = new JPanel(new GridLayout(1, 1));
        firstColumnPanel.setBorder(BorderFactory
                .createTitledBorder("First Column"));
        JPanel flowLayout = new JPanel(new FlowLayout());
        flowLayout.add(m_firstColumns);
        firstColumnPanel.add(flowLayout);

        JPanel secondColumnPanel = new JPanel(new GridLayout(1, 1));
        secondColumnPanel.setBorder(BorderFactory
                .createTitledBorder("Second Column"));
        flowLayout = new JPanel(new FlowLayout());
        flowLayout.add(m_secondColumns);

        secondColumnPanel.add(flowLayout);


        m_flowvariableBox = new JCheckBox("Use name prefix");
        m_flowVariablePrefixTextField = new JTextField(10);
        m_flowVariablePrefixTextField.setSize(new Dimension(10, 3));

        m_flowvariableBox.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent arg0) {
                if (m_flowvariableBox.isSelected()) {
                    m_flowVariablePrefixTextField.setEnabled(true);
                } else {
                    m_flowVariablePrefixTextField.setEnabled(false);
                }

            }
        });
        m_flowvariableBox.doClick(); // sync states


        JPanel thirdColumnPanel = new JPanel(new GridLayout(1, 1));
        thirdColumnPanel.setBorder(BorderFactory
                .createTitledBorder("Provide scores as flow variables"));
        flowLayout = new JPanel(new FlowLayout());
        flowLayout.add(m_flowvariableBox);
        flowLayout.add(m_flowVariablePrefixTextField);
        thirdColumnPanel.add(flowLayout);


        p.add(firstColumnPanel);

        p.add(secondColumnPanel);

        p.add(m_sortingOptions);

        p.add(thirdColumnPanel);

        final ItemListener colChangeListener = new ItemListener() {
            @Override
            public void itemStateChanged(final ItemEvent e) {
                DataColumnSpec specFirst = (DataColumnSpec)m_firstColumns.getSelectedItem();
                DataColumnSpec specSecond = (DataColumnSpec)m_secondColumns.getSelectedItem();
                if (specFirst == null || specSecond == null) {
                    return;
                }
                if (specFirst.getType().isCompatible(DoubleValue.class)
                    && specSecond.getType().isCompatible(DoubleValue.class)) {
                    m_sortingOptions.setPossibleSortingStrategies(SUPPORTED_NUMBER_SORT_STRATEGIES);
                } else if (specFirst.getType().isCompatible(StringValue.class)
                    && specSecond.getType().isCompatible(StringValue.class)) {
                    m_sortingOptions.setPossibleSortingStrategies(SUPPORTED_STRING_SORT_STRATEGIES);
                } else {
                    m_sortingOptions.setPossibleSortingStrategies(SortingStrategy.Lexical);
                }
            }
        };
        m_firstColumns.addItemListener(colChangeListener);
        m_secondColumns.addItemListener(colChangeListener);
        m_sortingOptions.updateControls();
        super.addTab("Spark Scorer", p);
    } // ScorerNodeDialog(NodeModel)

    /**
     * Fills the two combo boxes with all column names retrieved from the input
     * table spec. The second and last column will be selected by default unless
     * the settings object contains others.
     *
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
        final PortObjectSpec[] specs) throws NotConfigurableException {

        if (specs == null || specs.length <= 0 || specs[SparkAccuracyScorerNodeModel.INPORT] == null) {
            throw new NotConfigurableException("No input Spark RDD available");
        }
        final DataTableSpec spec = ((SparkDataPortObjectSpec) specs[SparkAccuracyScorerNodeModel.INPORT]).getTableSpec();

        if ((spec == null) || (spec.getNumColumns() < 2)) {
            throw new NotConfigurableException("Scorer needs an input table "
                    + "with at least two columns");
        }

        m_firstColumns.removeAllItems();
        m_secondColumns.removeAllItems();

        int numCols = spec.getNumColumns();
        for (int i = 0; i < numCols; i++) {
            DataColumnSpec c = spec.getColumnSpec(i);
            m_firstColumns.addItem(c);
            m_secondColumns.addItem(c);
        }
        // if at least two columns available
        String col2DefaultName = (numCols > 0) ? spec.getColumnSpec(numCols - 1).getName() : null;
        String col1DefaultName = (numCols > 1) ? spec.getColumnSpec(numCols - 2).getName() : col2DefaultName;
        DataColumnSpec col1 =
            spec.getColumnSpec(settings.getString(SparkAccuracyScorerNodeModel.FIRST_COMP_ID, col1DefaultName));
        DataColumnSpec col2 =
            spec.getColumnSpec(settings.getString(SparkAccuracyScorerNodeModel.SECOND_COMP_ID, col2DefaultName));
        m_firstColumns.setSelectedItem(col1);
        m_secondColumns.setSelectedItem(col2);

        String varPrefix = settings.getString(
        		SparkAccuracyScorerNodeModel.FLOW_VAR_PREFIX, null);

        boolean useFlowVar = varPrefix != null;

        if (m_flowvariableBox.isSelected() != useFlowVar) {
        	m_flowvariableBox.doClick();
        }
        if (varPrefix != null) {
        	m_flowVariablePrefixTextField.setText(varPrefix);
        }

        try {
            m_sortingOptions.loadDefault(settings);
        } catch (InvalidSettingsException e) {
            m_sortingOptions.setSortingStrategy(SortingStrategy.Lexical);
            m_sortingOptions.setReverseOrder(false);
        }
        m_sortingOptions.updateControls();
    }

    /**
     * Sets the selected columns inside the {@link SparkAccuracyScorerNodeModel}.
     *
     * @param settings the object to write the settings into
     * @throws InvalidSettingsException if the column selection is invalid
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {

        assert (settings != null);

        String firstColumn =
                ((DataColumnSpec)m_firstColumns.getSelectedItem()).getName();
        String secondColumn =
                ((DataColumnSpec)m_secondColumns.getSelectedItem()).getName();

        if ((firstColumn == null) || (secondColumn == null)) {
            throw new InvalidSettingsException("Select two valid column names "
                    + "from the lists (or press cancel).");
        }
        if (m_firstColumns.getItemCount() > 1
                && firstColumn.equals(secondColumn)) {
            throw new InvalidSettingsException(
                    "First and second column cannot be the same.");
        }

        settings.addString(SparkAccuracyScorerNodeModel.FIRST_COMP_ID, firstColumn);
        settings.addString(SparkAccuracyScorerNodeModel.SECOND_COMP_ID, secondColumn);

        boolean useFlowVar = m_flowvariableBox.isSelected();

        String flowVariableName = m_flowVariablePrefixTextField.getText();

        settings.addString(SparkAccuracyScorerNodeModel.FLOW_VAR_PREFIX,
        		useFlowVar ? flowVariableName : null);

        m_sortingOptions.saveDefault(settings);
    }
}
