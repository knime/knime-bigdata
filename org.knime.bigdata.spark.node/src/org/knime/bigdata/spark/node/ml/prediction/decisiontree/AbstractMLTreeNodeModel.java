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
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import java.util.Random;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.bigdata.spark.core.node.SparkMLModelLearnerNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObject;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Ole Ostergaard
 * @param <I> The {@link JobInput}
 * @param <T> The {@link MLlibNodeSettings}
 */
public abstract class AbstractMLTreeNodeModel<I extends MLDecisionTreeLearnerJobInput, T extends DecisionTreeSettings>
    extends SparkMLModelLearnerNodeModel<I, T> {

    /** Index of input data port. */
    static final int DATA_INPORT = 0;

    /**
     * @param modelType
     * @param jobId
     * @param requireClassCol
     */
    public AbstractMLTreeNodeModel(final MLModelType modelType, final String jobId, final boolean requireClassCol) {
        super(modelType, jobId, requireClassCol);
    }

    /**
     * @param inPortTypes
     * @param outPortTypes
     * @param modelType
     * @param jobId
     * @param requireClassCol
     */
    public AbstractMLTreeNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes, final MLModelType modelType, final String jobId,
        final boolean requireClassCol) {
        super(inPortTypes, outPortTypes, modelType, jobId, requireClassCol);
    }

    /**
     * @param modelType
     * @param jobId
     * @param settings
     */
    public AbstractMLTreeNodeModel(final MLModelType modelType, final String jobId,
        final T settings) {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkMLModelPortObject.PORT_TYPE}, modelType, jobId,
            settings);
    }
}