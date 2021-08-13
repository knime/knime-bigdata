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
 */
package org.knime.bigdata.spark.node.ml.prediction.linear.regression;

import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType.Category;
import org.knime.bigdata.spark.node.ml.prediction.linear.AbstractMLLinearLearnerNodeModel;
import org.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;

/**
 * ML-based Spark linear regression learner model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLLinearRegressionLearnerNodeModel
    extends AbstractMLLinearLearnerNodeModel<MLLinearRegressionLearnerJobInput, LinearLearnerSettings> {

    /** Unique model name. */
    public static final String MODEL_NAME = "MLLinearRegression";

    /** The model type. */
    public static final MLModelType MODEL_TYPE = MLModelType.getOrCreate(Category.REGRESSION, MODEL_NAME);

    /** Unique job id. */
    public static final String JOB_ID = "MLLinearRegressionLearnerJob";

    /**
     * Constructor.
     */
    public MLLinearRegressionLearnerNodeModel() {
        super(MODEL_TYPE, JOB_ID, new LinearLearnerSettings(LinearLearnerMode.LINEAR_REGRESSION));
    }

    @Override
    protected MLLinearRegressionLearnerJobInput createJobInput(final PortObject[] inData, final String newNamedModelId,
        final LinearLearnerSettings settings) throws InvalidSettingsException {

        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final MLlibSettings mlSettings = settings.getSettings(data);

        return new MLLinearRegressionLearnerJobInput( //
            data.getTableName(), //
            newNamedModelId, //
            mlSettings.getClassColIdx(), //
            mlSettings.getFeatueColIdxs(), //
            settings.getLossFunction().toSparkName(), //
            settings.getMaxIter(), //
            settings.useStandardization(), //
            settings.fitIntercept(), //
            settings.getRegularizer().name(), //
            settings.getRegParam(), //
            settings.getElasticNetParam(), //
            settings.getSolver().toSparkName(), //
            settings.getConvergenceTolerance());
    }

}
