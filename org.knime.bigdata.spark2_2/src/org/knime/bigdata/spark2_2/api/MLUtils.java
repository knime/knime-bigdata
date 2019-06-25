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
 *   Created on May 31, 2019 by bjoern
 */
package org.knime.bigdata.spark2_2.api;

import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLUtils {

    public static <T extends PipelineStage> T findFirstStageOfType(final PipelineModel pipelineModel,
        final Class<T> stageType) {
        for (Transformer stage : pipelineModel.stages()) {
            if (stageType.isAssignableFrom(stage.getClass())) {
                return (T)stage;
            }
        }
        throw new IllegalArgumentException("Pipeline model does not contain a " + stageType.getSimpleName());
    }

    /**
     * Adds nominal value mappings for all feature columns and the target column (if applicable) to the given
     * {@link MLMetaData}. The given pipeline model is assumed to consist of 0..n {@link StringIndexerModel} stages
     * followed by one {@link VectorAssembler}. The actual regression/classification model will not be looked at.
     *
     * @param model The fitted pipeline model.
     * @param metaData Meta data object to add nominal value mappings to.
     */
    public static void addNominalValueMappingsToMetaData(final PipelineModel model, final MLMetaData metaData) {

        final List<String> featureColumns =
            Arrays.asList(findFirstStageOfType(model, VectorAssembler.class).getInputCols());

        for (PipelineStage stage : model.stages()) {
            if (stage instanceof StringIndexerModel) {
                StringIndexerModel stringIndexer = (StringIndexerModel)stage;

                // featureIdx is the index of the feature in the feature vectors produced by the VectorAssembler stage
                int featureIdx = featureColumns.indexOf(stringIndexer.getOutputCol());

                if (featureIdx != -1) {
                    metaData.withNominalFeatureValueMapping(featureIdx, Arrays.asList(stringIndexer.labels()));
                } else {
                    // StringIndexerModel was not for a feature column, hence it must be for the target column when
                    // doing classification.
                    metaData.withNominalTargetValueMappings(Arrays.asList(stringIndexer.labels()));
                }
            }
        }
    }

    public static Dataset<Row> retainRowsWithMissingValuesInFeatures(final Dataset<Row> inputDataset,
        final Iterable<String> features) {

        Column orIsNulls = lit(false);
        for (String feature : features) {
            orIsNulls = orIsNulls.or(column(feature).isNull());
        }

        return inputDataset.where(orIsNulls);
    }

    public static Dataset<Row> retainRowsWithoutMissingValuesInFeatures(final Dataset<Row> inputDataset,
        final Iterable<String> features) {

        Column andIsNotNull = lit(true);

        for (String feature : features) {
            andIsNotNull = andIsNotNull.and(column(feature).isNotNull());
        }

        return inputDataset.where(andIsNotNull);
    }

}
