/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark.node.scorer.accuracy;

import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.node.mine.scorer.accuracy.AccuracyScorerNodeModel;
import org.knime.base.node.mine.scorer.accuracy.ScorerViewData;
import org.knime.base.util.SortingOptionPanel;
import org.knime.base.util.SortingStrategy;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Node model for Spark scorer. Provides the same settings as the regular {@link AccuracyScorerNodeModel}, except that it does not
 * support insertion order sorting (due to vanilla Spark RDDs not having an order).
 *
 * TODO: Remove code duplicates with {@link AccuracyScorerNodeModel} as much as possible
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkAccuracyScorerNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkAccuracyScorerNodeModel.class.getCanonicalName();

    /** The node logger for this class. */
    protected static final NodeLogger LOGGER = NodeLogger.getLogger(SparkAccuracyScorerNodeModel.class);

    /** Identifier in model spec to address first column name to compare. */
    static final String FIRST_COMP_ID = "first";

    /** Identifier in model spec to address first second name to compare. */
    static final String SECOND_COMP_ID = "second";

    /** Identifier in model spec to address the chosen prefix. */
    static final String FLOW_VAR_PREFIX = "flowVarPrefix";

    /** Identifier in model spec to address sorting strategy of labels. */
    static final String SORTING_STRATEGY = SortingOptionPanel.DEFAULT_KEY_SORTING_STRATEGY;

    /** Identifier in model spec to address sorting order of labels. */
    static final String SORTING_REVERSED = SortingOptionPanel.DEFAULT_KEY_SORTING_REVERSED;

    /** The input port 0. */
    static final int INPORT = 0;

    /** The output port 0: confusion matrix. */
    static final int OUTPORT_0 = 0;

    /** The output port 1: accuracy measures. */
    static final int OUTPORT_1 = 1;

    private static final DataColumnSpec[] QUALITY_MEASURES_SPECS =
        new DataColumnSpec[]{new DataColumnSpecCreator("TruePositives", IntCell.TYPE).createSpec(),
            new DataColumnSpecCreator("FalsePositives", IntCell.TYPE).createSpec(),
            new DataColumnSpecCreator("TrueNegatives", IntCell.TYPE).createSpec(),
            new DataColumnSpecCreator("FalseNegatives", IntCell.TYPE).createSpec(),
            new DataColumnSpecCreator("Recall", DoubleCell.TYPE).createSpec(),
            new DataColumnSpecCreator("Precision", DoubleCell.TYPE).createSpec(),
            new DataColumnSpecCreator("Sensitivity", DoubleCell.TYPE).createSpec(),
            new DataColumnSpecCreator("Specifity", DoubleCell.TYPE).createSpec(),
            new DataColumnSpecCreator("F-measure", DoubleCell.TYPE).createSpec(),
            new DataColumnSpecCreator("Accuracy", DoubleCell.TYPE).createSpec(),
            new DataColumnSpecCreator("Cohen's kappa", DoubleCell.TYPE).createSpec()};

    /** The name of the first column to compare. */
    private String m_firstCompareColumn;

    /** The name of the second column to compare. */
    private String m_secondCompareColumn;

    /** The prefix added to the flow variable names. **/
    private String m_flowVarPrefix;

    /** Sort strategy to use for labels in compare column **/
    private SortingStrategy m_sortingStrategy = SortingStrategy.Lexical;

    /** Whether to reverse the sort order of labels in compare column **/
    private boolean m_sortingReversed;

    private ScorerViewData m_viewData;

    /** Constructor. */
    public SparkAccuracyScorerNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE, BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final DataTableSpec inSpec = ((SparkDataPortObjectSpec)inSpecs[INPORT]).getTableSpec();

        if (inSpec.getNumColumns() < 2) {
            throw new InvalidSettingsException("The input table must have at least two colums to compare");
        }
        if (StringUtils.isBlank(m_firstCompareColumn) || StringUtils.isBlank(m_secondCompareColumn)) {
            throw new InvalidSettingsException("No columns selected.");
        }
        if (!inSpec.containsName(m_firstCompareColumn)) {
            throw new InvalidSettingsException("Column " + m_firstCompareColumn + " not found.");
        }
        if (!inSpec.containsName(m_secondCompareColumn)) {
            throw new InvalidSettingsException("Column " + m_secondCompareColumn + " not found.");
        }

        pushFlowVars(true);
        return new DataTableSpec[]{null, new DataTableSpec(QUALITY_MEASURES_SPECS)};
    }

    /**
     * Pushes the results to flow variables.
     *
     * @param isConfigureOnly true enable overwriting check
     */
    private void pushFlowVars(final boolean isConfigureOnly) {
        Map<String, FlowVariable> vars = getAvailableFlowVariables();
        String prefix = m_flowVarPrefix != null ? m_flowVarPrefix : "";
        String accuracyName = prefix + "Accuracy";
        String errorName = prefix + "Error";
        String correctName = prefix + "#Correct";
        String falseName = prefix + "#False";
        String kappaName = prefix + "Cohen's kappa";
        if (isConfigureOnly && (vars.containsKey(accuracyName) || vars.containsKey(errorName)
            || vars.containsKey(correctName) || vars.containsKey(falseName) || vars.containsKey(kappaName))) {
            setWarningMessage("A flow variable was replaced!");
        }

        double accu = isConfigureOnly ? 0.0 : m_viewData.getAccuracy();
        double error = isConfigureOnly ? 0.0 : m_viewData.getError();
        int correctCount = isConfigureOnly ? 0 : m_viewData.getCorrectCount();
        int falseCount = isConfigureOnly ? 0 : m_viewData.getFalseCount();
        double kappa = isConfigureOnly ? 0 : m_viewData.getCohenKappa();
        pushFlowVariableDouble(accuracyName, accu);
        pushFlowVariableDouble(errorName, error);
        pushFlowVariableInt(correctName, correctCount);
        pushFlowVariableInt(falseName, falseCount);
        pushFlowVariableDouble(kappaName, kappa);
    }

    /**
     * Resets all internal data.
     */
    @Override
    protected void resetInternal() {
        m_viewData = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject inPort = (SparkDataPortObject)inData[0];
        final DataTableSpec tableSpec = inPort.getTableSpec();
        final SparkContextID contextID = inPort.getContextID();

        final JobRunFactory<ScorerJobInput, ScorerJobOutput> runFactory = SparkContextUtil.getJobRunFactory(contextID, JOB_ID);
        final ScorerJobInput jobInput = new ScorerJobInput(inPort.getTableName(), tableSpec.findColumnIndex(m_firstCompareColumn),
            tableSpec.findColumnIndex(m_secondCompareColumn));
        final ScorerJobOutput jobOutput = runFactory.createRun(jobInput).run(contextID, exec);

        if (jobOutput.getRowCount() == 0) {
            throw new IllegalArgumentException("Empty input Spark DataFrame/RDD.");
        }

        m_viewData = createViewData(jobOutput, tableSpec);

        pushFlowVars(false);

        return new BufferedDataTable[]{createConfusionMatrixTable(m_viewData, exec),
            createAccuracyStatisticsTable(m_viewData, exec)};
    }

    private ScorerViewData createViewData(final ScorerJobOutput jobOutput, final DataTableSpec inTableSpec) {
        // sort target values
        final Object[] sortedTargetValues = jobOutput.getLabels().toArray().clone();
        Arrays.sort(sortedTargetValues, createComparator(inTableSpec));

        // sort scorer counts accordingly
        final int[][] scorerCountSorted = getSortedScorerCounts(jobOutput, sortedTargetValues);

        return new ScorerViewData(scorerCountSorted, (int)jobOutput.getRowCount(), // TODO: the int conversion here might be a problem for very large Spark RDDs.
            jobOutput.getFalseCount(), jobOutput.getCorrectCount(), m_firstCompareColumn, m_secondCompareColumn,
            toStringArray(sortedTargetValues));
    }

    private int[][] getSortedScorerCounts(final ScorerJobOutput jobOutput, final Object[] sortedTargetValues) {
        // memorize target value indices
        final Object[] origTargetValues = jobOutput.getLabels().toArray();
        final int valueCount = origTargetValues.length;

        Map<Object, Integer> originalIdx = new HashMap<>();
        for (int i = 0; i < valueCount; i++) {
            originalIdx.put(origTargetValues[i], i);
        }

        // rearrange scorer counts according to sorted target values
        final int[][] scorerCount = jobOutput.getConfusionMatrix();
        final int[][] scorerCountSorted = new int[valueCount][valueCount];
        for (int i = 0; i < valueCount; i++) {
            for (int j = 0; j < valueCount; j++) {
                scorerCountSorted[i][j] =
                    scorerCount[originalIdx.get(sortedTargetValues[i])][originalIdx.get(sortedTargetValues[j])];
            }
        }

        return scorerCountSorted;
    }

    private Comparator<Object> createComparator(final DataTableSpec inTableSpec) {
        final Comparator<Object> comparator;
        switch (m_sortingStrategy) {
            case InsertionOrder:
            case Unsorted:
                throw new IllegalStateException(
                    "'Insertion order' and 'Unsorted' sorting strategies are not supported.");
            case Lexical:
                comparator = createLexicalSortingComparator();
                break;
            case Numeric:
                DataType type1 = inTableSpec.getColumnSpec(m_firstCompareColumn).getType();
                DataType type2 = inTableSpec.getColumnSpec(m_secondCompareColumn).getType();
                DataType type = DataType.getCommonSuperType(type1, type2);
                if (!DoubleCell.TYPE.isASuperTypeOf(type)) {
                    throw new IllegalStateException("Numerical sorting strategy is not supported.");
                }
                comparator = createNumericalSortingComparator();
                break;
            default:
                throw new IllegalStateException("Unrecognized sorting strategy: " + m_sortingStrategy);
        }
        return comparator;
    }

    private Comparator<Object> createNumericalSortingComparator() {
        final int sign = (m_sortingReversed) ? -1 : 1;
        return new Comparator<Object>() {
            @Override
            public int compare(final Object o1, final Object o2) {
                return sign * Double.compare(getDouble(o1), getDouble(o2));
            }
        };
    }

    private double getDouble(final Object o) {
        if (o instanceof Number) {
            return ((Number)o).doubleValue();
        } else if (o instanceof Boolean) {
            return ((boolean)o) ? 1d : 0d;
        } else {
            throw new IllegalArgumentException("Unsopported data type for numerical sorting: " + o.getClass());
        }
    }

    @SuppressWarnings("unchecked")
    private Comparator<Object> createLexicalSortingComparator() {
        Collator instance = Collator.getInstance();
        //do not try to combine characters
        instance.setDecomposition(Collator.NO_DECOMPOSITION);
        //case and accents matter.
        instance.setStrength(Collator.IDENTICAL);

        final Comparator<String> stringComparator = (Comparator<String>)(Comparator<?>)instance;
        final int sign = (m_sortingReversed) ? -1 : 1;

        return new Comparator<Object>() {
            @Override
            public int compare(final Object o1, final Object o2) {
                return sign * stringComparator.compare(o1.toString(), o2.toString());
            }
        };
    }

    private BufferedDataTable createConfusionMatrixTable(final ScorerViewData viewData,
        final ExecutionContext exec) {
        final String[] targetValues = viewData.getTargetValues();
        final int[][] scorerCount = viewData.getScorerCount();

        DataType[] colTypes = new DataType[targetValues.length];
        Arrays.fill(colTypes, IntCell.TYPE);
        BufferedDataContainer container =
            exec.createDataContainer(new DataTableSpec(viewData.getTargetValues(), colTypes));
        for (int i = 0; i < targetValues.length; i++) {
            // need to make a datacell for the row key
            container.addRowToTable(new DefaultRow(targetValues[i], scorerCount[i]));
        }
        container.close();
        return container.getTable();
    }

    private BufferedDataTable createAccuracyStatisticsTable(final ScorerViewData viewData,
        final ExecutionContext exec) {
        String[] targetValues = viewData.getTargetValues();

        BufferedDataContainer accTable = exec.createDataContainer(new DataTableSpec(QUALITY_MEASURES_SPECS));
        for (int r = 0; r < targetValues.length; r++) {
            int tp = viewData.getTP(r); // true positives
            int fp = viewData.getFP(r); // false positives
            int tn = viewData.getTN(r); // true negatives
            int fn = viewData.getFN(r); // false negatives
            final DataCell sensitivity; // TP / (TP + FN)
            DoubleCell recall = null; // TP / (TP + FN)
            if (tp + fn > 0) {
                recall = new DoubleCell(1.0 * tp / (tp + fn));
                sensitivity = new DoubleCell(1.0 * tp / (tp + fn));
            } else {
                sensitivity = DataType.getMissingCell();
            }
            DoubleCell prec = null; // TP / (TP + FP)
            if (tp + fp > 0) {
                prec = new DoubleCell(1.0 * tp / (tp + fp));
            }
            final DataCell specificity; // TN / (TN + FP)
            if (tn + fp > 0) {
                specificity = new DoubleCell(1.0 * tn / (tn + fp));
            } else {
                specificity = DataType.getMissingCell();
            }
            final DataCell fmeasure; // 2 * Prec. * Recall / (Prec. + Recall)
            if (recall != null && prec != null) {
                fmeasure = new DoubleCell(2.0 * prec.getDoubleValue() * recall.getDoubleValue()
                    / (prec.getDoubleValue() + recall.getDoubleValue()));
            } else {
                fmeasure = DataType.getMissingCell();
            }
            // add complete row for class value to table
            DataRow row = new DefaultRow(new RowKey(targetValues[r]),
                new DataCell[]{new IntCell(tp), new IntCell(fp), new IntCell(tn), new IntCell(fn),
                    recall == null ? DataType.getMissingCell() : recall,
                    prec == null ? DataType.getMissingCell() : prec, sensitivity, specificity, fmeasure,
                    DataType.getMissingCell(), DataType.getMissingCell()});
            accTable.addRowToTable(row);
        }
        List<String> classIds = Arrays.asList(targetValues);
        RowKey overallID = new RowKey("Overall");
        int uniquifier = 1;
        while (classIds.contains(overallID.getString())) {
            overallID = new RowKey("Overall (#" + (uniquifier++) + ")");
        }
        // append additional row for overall accuracy
        accTable.addRowToTable(new DefaultRow(overallID,
            new DataCell[]{DataType.getMissingCell(), DataType.getMissingCell(), DataType.getMissingCell(),
                DataType.getMissingCell(), DataType.getMissingCell(), DataType.getMissingCell(),
                DataType.getMissingCell(), DataType.getMissingCell(), DataType.getMissingCell(),
                new DoubleCell(viewData.getAccuracy()), new DoubleCell(viewData.getCohenKappa())}));
        accTable.close();

        return accTable.getTable();
    }

    private String[] toStringArray(final Object[] targetValues) {
        final String[] ret = new String[targetValues.length];
        for (int i = 0; i < targetValues.length; i++) {
            if (targetValues[i] != null) {
                ret[i] = targetValues[i].toString();
            }
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        if (m_firstCompareColumn != null) {
            settings.addString(FIRST_COMP_ID, m_firstCompareColumn);
        }
        if (m_secondCompareColumn != null) {
            settings.addString(SECOND_COMP_ID, m_secondCompareColumn);
        }

        settings.addString(FLOW_VAR_PREFIX, m_flowVarPrefix);
        settings.addBoolean(SORTING_REVERSED, m_sortingReversed);
        settings.addInt(SORTING_STRATEGY, m_sortingStrategy.ordinal());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(FIRST_COMP_ID);
        settings.getString(SECOND_COMP_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_firstCompareColumn = settings.getString(FIRST_COMP_ID);
        m_secondCompareColumn = settings.getString(SECOND_COMP_ID);

        // added in 2.6
        m_flowVarPrefix = settings.getString(FLOW_VAR_PREFIX, null);

        // Added sorting strategy, reversed are new in 2.9.0
        m_sortingReversed = settings.getBoolean(SORTING_REVERSED, false);
        m_sortingStrategy =
            SortingStrategy.values()[settings.getInt(SORTING_STRATEGY, SortingStrategy.Lexical.ordinal())];
    }

    /**
     * Returns the data that should be displayed in the node's view. May be null if the data has not been computed in
     * {@link #execute(BufferedDataTable[], ExecutionContext)} yet.
     *
     * @return the view data or <code>null</code>
     */
    public ScorerViewData getViewData() {
        return m_viewData;
    }
}
