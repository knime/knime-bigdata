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
 *   Created on May 26, 2016 by oole
 */
package org.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.io.Serializable;

import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMappings;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.bigdata.spark.core.node.SparkModelLearnerNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;

/**
 *
 * @author Ole Ostergaard
 * @param <I> The {@link JobInput}
 * @param <T> The {@link MLlibNodeSettings}
 */
public abstract class AbstractMLlibTreeNodeModel<I extends DecisionTreeJobInput, T extends DecisionTreeSettings>
    extends SparkModelLearnerNodeModel<I, T> {

    /** Index of input data port. */
    static final int DATA_INPORT = 0;

    /**
     * @param modelName
     * @param jobId
     * @param requireClassCol
     */
    public AbstractMLlibTreeNodeModel(final String modelName, final String jobId, final boolean requireClassCol) {
        super(modelName, jobId, requireClassCol);
    }

    /**
     * @param inPortTypes
     * @param outPortTypes
     * @param modelName
     * @param jobId
     * @param requireClassCol
     */
    public AbstractMLlibTreeNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes, final String modelName, final String jobId,
        final boolean requireClassCol) {
        super(inPortTypes, outPortTypes, modelName, jobId, requireClassCol);
    }

    /**
     * @param modelName
     * @param jobId
     * @param settings
     */
    public AbstractMLlibTreeNodeModel(final String modelName, final String jobId,
        final T settings) {
        super(new PortType[]{SparkDataPortObject.TYPE, PMMLPortObject.TYPE_OPTIONAL},
            new PortType[]{SparkModelPortObject.TYPE}, modelName, jobId, settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SparkModelPortObject createSparkModelPortObject(final PortObject[] inData, final T settings,
        final ModelJobOutput result) throws MissingSparkModelHelperException, InvalidSettingsException {

        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        Serializable metaData;
        if (inData.length == 2 && inData[1] != null) {
            final PMMLPortObject pmml = (PMMLPortObject)inData[1];
            final ColumnBasedValueMapping numberMap = ColumnBasedValueMappings.fromPMMLPortObject(pmml, data);
            metaData = numberMap;
        } else {
            metaData = null;
        }
        return new SparkModelPortObject(new SparkModel(getSparkVersion(data), getModelName(), result.getModel(),
            settings.getSettings(data), metaData));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected I createJobInput(final PortObject[] inData, final T settings)
        throws InvalidSettingsException {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final PMMLPortObject mapping = (PMMLPortObject)inData[1];
        final MLlibSettings mllibSettings = settings.getSettings(data, mapping);
        return getJob(settings, data, mllibSettings);
    }

    /**
     * @param settings
     * @param data
     * @param mllibSettings
     * @return the jobinput
     */
    abstract protected I getJob(final T settings, final SparkDataPortObject data,
        final MLlibSettings mllibSettings);

}