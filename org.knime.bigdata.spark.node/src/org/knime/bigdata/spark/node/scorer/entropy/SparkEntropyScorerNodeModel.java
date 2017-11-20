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
 *   Created on Sep 30, 2015 by bjoern
 */
package org.knime.bigdata.spark.node.scorer.entropy;

import java.util.Map;

import org.knime.base.node.mine.scorer.entrop.EntropyCalculator;
import org.knime.base.node.mine.scorer.entrop.EntropyNodeDialogPane;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.node.scorer.entropy.EntropyScorerData.ClusterScore;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.MissingCell;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Node model for the Spark Entropy Scorer node. Triggers a Spark job to compute the entropy score of a clustering.
 *
 * TODO: this has some code duplicates with {@link org.knime.base.node.mine.scorer.entrop.EntropyNodeModel}
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkEntropyScorerNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkEntropyScorerNodeModel.class.getCanonicalName();


    /** Config identifier: column name in reference table. */
    public static final String CFG_REFERENCE_COLUMN = "reference_table_col_column";

    /** Config identifier: column name in clustering table. */
    public static final String CFG_CLUSTERING_COLUMN = "clustering_table_col_column";

    private String m_referenceCol;

    private String m_clusteringCol;

    private SparkEntropyScorerViewData m_viewData;

    private final SettingsModelBoolean m_flowVarModel = new SettingsModelBoolean("generate flow variables", false);
    private final SettingsModelString m_useNamePrefixModel = EntropyNodeDialogPane.createFlowPrefixModel(m_flowVarModel);

    /**
     * Creates a new SparkEntropyScorerNodeModel instance with two SparkDataPorts as input, and one DataTable as output
     * port.
     */
    public SparkEntropyScorerNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (m_referenceCol == null || m_clusteringCol == null) {
            throw new InvalidSettingsException("No auto configuration available\n" + "Please configure in dialog.");

        }
        DataTableSpec spec = ((SparkDataPortObjectSpec)inSpecs[0]).getTableSpec();

        if (!spec.containsName(m_referenceCol)) {
            throw new InvalidSettingsException("Invalid reference column name " + m_referenceCol);
        }
        if (!spec.containsName(m_clusteringCol)) {
            throw new InvalidSettingsException("Invalid clustering column name " + m_clusteringCol);
        }
        pushFlowVars(true);
        return new DataTableSpec[]{EntropyCalculator.getScoreTableSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject inPort = (SparkDataPortObject)inData[0];
        final SparkContextID contextID = inPort.getContextID();
        final DataTableSpec tableSpec = inPort.getTableSpec();

        final JobRunFactory<EntropyScorerJobInput, EntropyScorerJobOutput> runFactory = SparkContextUtil.getJobRunFactory(contextID, JOB_ID);
        final EntropyScorerJobInput jobInput = new EntropyScorerJobInput(inPort.getTableName(),
            tableSpec.findColumnIndex(m_referenceCol), tableSpec.findColumnIndex(m_clusteringCol));
        final EntropyScorerJobOutput jobOutput = runFactory.createRun(jobInput).run(contextID, exec);

        m_viewData = new SparkEntropyScorerViewData(jobOutput, createQualityTable(jobOutput, exec));

        pushFlowVars(false);
        return new BufferedDataTable[]{m_viewData.getScoreTable()};
    }

    private BufferedDataTable createQualityTable(final EntropyScorerJobOutput jobOutput, final ExecutionContext exec) {
        BufferedDataContainer container = exec.createDataContainer(EntropyCalculator.getScoreTableSpec());

        for (ClusterScore clusterScore : jobOutput.getClusterScores()) {
            container.addRowToTable(new DefaultRow(new RowKey(clusterScore.getCluster().toString()),
                new DataCell[]{new IntCell(clusterScore.getSize()), // size
                    new DoubleCell(clusterScore.getEntropy()), // entropy
                    new DoubleCell(clusterScore.getNormalizedEntropy()), new MissingCell("?") // normalized entropy
            }));
        }

        container.addRowToTable(new DefaultRow(new RowKey("Overall"),
            new DataCell[]{new IntCell(jobOutput.getOverallSize()), // size
                new DoubleCell(jobOutput.getOverallEntropy()), // entropy
                new DoubleCell(jobOutput.getOverallNormalizedEntropy()), // normalized entropy
                new DoubleCell(jobOutput.getOverallQuality()) // quality
        }));

        container.close();
        return container.getTable();
    }

    /**
     * Pushes the results to flow variables.
     *
     * @param isConfigureOnly true enable overwriting check
     */
    private void pushFlowVars(final boolean isConfigureOnly) {

        if (m_flowVarModel.getBooleanValue()) {
            Map<String, FlowVariable> vars = getAvailableFlowVariables();

            String prefix = m_useNamePrefixModel.getStringValue();
            String overallQuality = prefix + "Overall quality";
            String overallEntropy = prefix + "Overall entropy";
            String overallNormalizedEntropy = prefix + "Overall normalized entropy";
            if (isConfigureOnly
                && (vars.containsKey(overallQuality) || vars.containsKey(overallEntropy) || vars.containsKey(overallNormalizedEntropy))) {
                addWarning("A flow variable was replaced!");
            }

            double quality = isConfigureOnly ? 0.0 : m_viewData.getOverallQuality();
            double entropy = isConfigureOnly ? 0.0 : m_viewData.getOverallEntropy();
            double normalizedEntropy = isConfigureOnly ? 0.0 : m_viewData.getOverallNormalizedEntropy();
            pushFlowVariableDouble(overallQuality, quality);
            pushFlowVariableDouble(overallEntropy, entropy);
            pushFlowVariableDouble(overallNormalizedEntropy, normalizedEntropy);
        }
    }

    /**
     * @param string
     */
    private void addWarning(final String string) {
        String warningMessage = getWarningMessage();
        if (warningMessage == null || warningMessage.isEmpty()) {
            setWarningMessage(string);
        } else {
            setWarningMessage(warningMessage + "\n" + string);
        }
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
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        if (m_referenceCol != null) {
            settings.addString(CFG_REFERENCE_COLUMN, m_referenceCol);
            settings.addString(CFG_CLUSTERING_COLUMN, m_clusteringCol);
        }
        m_flowVarModel.saveSettingsTo(settings);
        m_useNamePrefixModel.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_REFERENCE_COLUMN);
        settings.getString(CFG_CLUSTERING_COLUMN);
        m_flowVarModel.validateSettings(settings);
        m_useNamePrefixModel.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_referenceCol = settings.getString(CFG_REFERENCE_COLUMN);
        m_clusteringCol = settings.getString(CFG_CLUSTERING_COLUMN);

        // for old workflows
        if (settings.containsKey(m_useNamePrefixModel.getKey())) {
            m_useNamePrefixModel.loadSettingsFrom(settings);
        }
        if (settings.containsKey(m_flowVarModel.getConfigName())) {
            m_flowVarModel.loadSettingsFrom(settings);
        }
    }

    /**
     * Returns the data that should be displayed in the node's view. May be null if the data has not been computed in
     * {@link #execute(BufferedDataTable[], ExecutionContext)} yet.
     *
     * @return the view data or <code>null</code>
     */
    public SparkEntropyScorerViewData getViewData() {
        return m_viewData;
    }
}
