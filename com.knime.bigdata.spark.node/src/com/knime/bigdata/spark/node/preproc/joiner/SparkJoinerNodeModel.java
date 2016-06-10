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
 *   Created on 22.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.joiner;

import java.util.List;

import org.knime.base.node.preproc.joiner.Joiner;
import org.knime.base.node.preproc.joiner.Joiner2Settings;
import org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.util.SparkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJoinerNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkJoinerNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     */
    protected SparkJoinerNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[] {SparkDataPortObject.TYPE});
    }

    private final Joiner2Settings m_settings = new Joiner2Settings();

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec left = (SparkDataPortObjectSpec)inSpecs[0];
        final SparkDataPortObjectSpec right = (SparkDataPortObjectSpec)inSpecs[1];
        final SparkContextID context = left.getContextID();
        if (!context.equals(right.getContextID())) {
            throw new InvalidSettingsException(
                "Spark context of first input incompatible with Spark context of second input");
        }
        final Joiner joiner = new Joiner(left.getTableSpec(), right.getTableSpec(), m_settings);
        final PortObjectSpec[] spec =
                new PortObjectSpec[]{new SparkDataPortObjectSpec(context, joiner.getOutputSpec())};

        if (!joiner.getConfigWarnings().isEmpty()) {
            for (String warning : joiner.getConfigWarnings()) {
                setWarningMessage(warning);
            }
        }
        return spec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject left = (SparkDataPortObject)inData[0];
        final SparkDataPortObject right = (SparkDataPortObject)inData[1];
        final SparkContextID context = left.getContextID();
        if (!context.equals(right.getContextID())) {
            throw new InvalidSettingsException(
                "Spark context of first input incompatible with Spark context of second input");
        }
        final DataTableSpec leftSpec = left.getTableSpec();
        final DataTableSpec rightSpec = right.getTableSpec();
        final Joiner joiner = new Joiner(leftSpec, rightSpec, m_settings);
        final Integer[] leftJoinColumns = SparkUtil.getColumnIndices(leftSpec, m_settings.getLeftJoinColumns());
        final Integer[] rightJoinColumns = SparkUtil.getColumnIndices(rightSpec, m_settings.getRightJoinColumns());
        final List<String> leftIncluded = joiner.getLeftIncluded(leftSpec);
        final Integer[] leftIncludCols;
//        if (leftIncluded == null || leftIncluded.isEmpty()) {
//            leftIncludCols = new Integer[0];
//        } else {
            leftIncludCols = SparkUtil.getColumnIndices(leftSpec, leftIncluded);
//        }
        final List<String> rightIncluded = joiner.getRightIncluded(rightSpec);
        final Integer[] rightIncludCols;
//        if (rightIncluded == null || rightIncluded.isEmpty()) {
//            rightIncludCols = new Integer[0];
//        } else {
            rightIncludCols = SparkUtil.getColumnIndices(rightSpec, rightIncluded);
//        }

        final JoinMode joinMode = m_settings.getJoinMode();
        final com.knime.bigdata.spark.node.preproc.joiner.JoinMode sparkJoinMode =
                com.knime.bigdata.spark.node.preproc.joiner.JoinMode.fromKnimeJoinMode(joinMode.toString());
        final DataTableSpec outputSpec = joiner.getOutputSpec();
        final SparkDataTable resultTable = new SparkDataTable(context, outputSpec);

        final SimpleJobRunFactory<SparkJoinerJobInput> runFactory = SparkContextUtil.getSimpleRunFactory(context, JOB_ID);
        final SparkJoinerJobInput jobInput = new SparkJoinerJobInput(left.getData().getID(), right.getData().getID(), sparkJoinMode,
            leftJoinColumns, rightJoinColumns, leftIncludCols, rightIncludCols, resultTable.getID());
        runFactory.createRun(jobInput).run(context, exec);

        return new PortObject[] {new SparkDataPortObject(resultTable)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        Joiner2Settings s = new Joiner2Settings();
        s.loadSettings(settings);
        Joiner.validateSettings(s);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_settings.loadSettings(settings);
    }
}