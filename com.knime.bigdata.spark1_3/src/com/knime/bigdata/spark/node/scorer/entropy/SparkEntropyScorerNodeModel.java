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
 *   Created on Sep 30, 2015 by bjoern
 */
package com.knime.bigdata.spark.node.scorer.entropy;

import org.knime.base.node.mine.scorer.entrop.EntropyCalculator;
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
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.server.EntropyScorerData;
import com.knime.bigdata.spark.jobserver.server.EntropyScorerData.ClusterScore;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 * Node model for the Spark Entropy Scorer node. Triggers a Spark job to compute the entropy score of a clustering.
 *
 * TODO: this has some code duplicates with {@link org.knime.base.node.mine.scorer.entrop.EntropyNodeModel}
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkEntropyScorerNodeModel extends SparkNodeModel {

    /** Config identifier: column name in reference table. */
    public static final String CFG_REFERENCE_COLUMN = "reference_table_col_column";

    /** Config identifier: column name in clustering table. */
    public static final String CFG_CLUSTERING_COLUMN = "clustering_table_col_column";

    private String m_referenceCol;

    private String m_clusteringCol;

    private SparkEntropyScorerViewData m_viewData;

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
        return new DataTableSpec[]{EntropyCalculator.getScoreTableSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject inPort = (SparkDataPortObject)inData[0];
        final DataTableSpec tableSpec = inPort.getTableSpec();

        EntropyScorerData scoringResult = (EntropyScorerData)new EntropyScorerTask(inPort.getData(),
            tableSpec.findColumnIndex(m_referenceCol), tableSpec.findColumnIndex(m_clusteringCol)).execute(exec);

        m_viewData = new SparkEntropyScorerViewData(scoringResult, createQualityTable(scoringResult, exec));

        return new BufferedDataTable[]{m_viewData.getScoreTable()};
    }

    private BufferedDataTable createQualityTable(final EntropyScorerData scoringResult, final ExecutionContext exec) {
        BufferedDataContainer container = exec.createDataContainer(EntropyCalculator.getScoreTableSpec());

        for (ClusterScore clusterScore : scoringResult.getClusterScores()) {
            container.addRowToTable(new DefaultRow(new RowKey(clusterScore.getCluster().toString()),
                new DataCell[]{new IntCell(clusterScore.getSize()), // size
                    new DoubleCell(clusterScore.getEntropy()), // entropy
                    new DoubleCell(clusterScore.getNormalizedEntropy()), new MissingCell("?") // normalized entropy
            }));
        }

        container.addRowToTable(new DefaultRow(new RowKey("Overall"),
            new DataCell[]{new IntCell(scoringResult.getOverallSize()), // size
                new DoubleCell(scoringResult.getOverallEntropy()), // entropy
                new DoubleCell(scoringResult.getOverallNormalizedEntropy()), // normalized entropy
                new DoubleCell(scoringResult.getOverallQuality()) // quality
        }));

        container.close();
        return container.getTable();
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
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_referenceCol != null) {
            settings.addString(CFG_REFERENCE_COLUMN, m_referenceCol);
            settings.addString(CFG_CLUSTERING_COLUMN, m_clusteringCol);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_REFERENCE_COLUMN);
        settings.getString(CFG_CLUSTERING_COLUMN);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_referenceCol = settings.getString(CFG_REFERENCE_COLUMN);
        m_clusteringCol = settings.getString(CFG_CLUSTERING_COLUMN);
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
