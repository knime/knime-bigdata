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

import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.node.mllib.SparkUtil;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJoinerNodeModel extends AbstractSparkNodeModel {

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
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec left = (SparkDataPortObjectSpec)inSpecs[0];
        final SparkDataPortObjectSpec reight = (SparkDataPortObjectSpec)inSpecs[1];
        final KNIMESparkContext context = left.getContext();
        if (!context.compatible(reight.getContext())) {
            throw new InvalidSettingsException(
                "Spark context of first input incompatible with Spark context of second input");
        }
        final Joiner joiner = new Joiner(left.getTableSpec(), reight.getTableSpec(), m_settings);
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
        final KNIMESparkContext context = left.getContext();
        if (!context.compatible(right.getContext())) {
            throw new InvalidSettingsException(
                "Spark context of first input incompatible with Spark context of second input");
        }
        final DataTableSpec leftSpec = left.getTableSpec();
        final DataTableSpec rightSpec = right.getTableSpec();
        final Joiner joiner = new Joiner(leftSpec, rightSpec, m_settings);
        final int[] leftJoinColumns = SparkUtil.getColumnIndices(leftSpec, m_settings.getLeftJoinColumns());
        final int[] rightJoinColumns = SparkUtil.getColumnIndices(rightSpec, m_settings.getRightJoinColumns());
        final Integer[] leftIncludCols = SparkUtil.getColumnIndices(leftSpec, joiner.getLeftIncluded(leftSpec));
        final Integer[] rightIncludCols = SparkUtil.getColumnIndices(rightSpec, joiner.getRightIncluded(leftSpec));
        final JoinMode joinMode = m_settings.getJoinMode();
        final DataTableSpec outputSpec = joiner.getOutputSpec();
        final SparkDataTable result = new SparkDataTable(context, outputSpec);
        return new PortObject[] {new SparkDataPortObject(result)};
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
