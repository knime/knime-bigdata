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
package org.knime.bigdata.spark3_0.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLUtils {

    /**
     * Find first the first stage of given type in the given pipeline or throw an missing type exception otherwise.
     *
     * @param <T> type of the stage to find
     * @param pipelineModel pipeline to search in
     * @param stageType type of the stage to find
     * @return first stage of given type found in the pipeline
     * @throws IllegalArgumentException if pipeline does not contain type
     */
    public static <T extends PipelineStage> T findFirstStageOfType(final PipelineModel pipelineModel,
        final Class<T> stageType) {

        return findFirstOptionalStageOfType(pipelineModel, stageType) //
            .orElseThrow(() -> new IllegalArgumentException("Pipeline model does not contain a " + stageType.getSimpleName()));
    }

    /**
     * Find first the first stage of given type in the given pipeline or return empty optional.
     *
     * @param <T> type of the stage to find
     * @param pipelineModel pipeline to search in
     * @param stageType type of the stage to find
     * @return first stage of given type found in the pipeline or empty optional
     */
    public static <T extends PipelineStage> Optional<T> findFirstOptionalStageOfType(final PipelineModel pipelineModel,
        final Class<T> stageType) {

        for (Transformer stage : pipelineModel.stages()) {
            if (stageType.isAssignableFrom(stage.getClass())) {
                return Optional.of((T)stage);
            }
        }

        return Optional.empty();
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

    /**
     * Adds nominal feature values for all feature columns and value mappings for the target column (if applicable) to
     * the given {@link MLMetaData}. The given pipeline model is assumed to consist of 0..n {@link StringIndexerModel}
     * stages, followed by on {@link OneHotEncoder}, followed by one {@link VectorAssembler}. The actual
     * regression/classification model will not be looked at.
     *
     * @param model The fitted pipeline model.
     * @param metaData Meta data object to add nominal value mappings to.
     */
    public static void addNominalFeatureValuesMappingsToMetaData(final PipelineModel model, final MLMetaData metaData) { // NOSONAR ignore complexity
        final VectorAssembler vectorAssembler = findFirstStageOfType(model, VectorAssembler.class);
        final Optional<OneHotEncoderModel> oneHotEncoder = findFirstOptionalStageOfType(model, OneHotEncoderModel.class);
        final List<String> oneHotEncoderInputCols = oneHotEncoder //
                .map(encoder -> Arrays.asList(encoder.getInputCols())) //
                .orElse(new ArrayList<>(0));
        final List<String> oneHotEncoderOutputCols = oneHotEncoder //
                .map(encoder -> Arrays.asList(encoder.getOutputCols())) //
                .orElse(new ArrayList<>(0));
        final String onHotEncoderHandleInvalid = oneHotEncoder //
                .map(OneHotEncoderModel::getHandleInvalid) //
                .orElse("");

        // find string indexer by one hot encoder output column / vector assembler input column
        final HashMap<String, StringIndexerModel> oneHotStringIndexer = new HashMap<>();
        for (final Transformer stage : model.stages()) {
            if (stage instanceof StringIndexerModel) {
                final StringIndexerModel stringIndexer = (StringIndexerModel)stage;

                final int oneHotEncoderColIndex = oneHotEncoderInputCols.indexOf(stringIndexer.getOutputCol());

                if (oneHotEncoderColIndex != -1) {
                    oneHotStringIndexer.put(oneHotEncoderOutputCols.get(oneHotEncoderColIndex), stringIndexer);
                } else {
                    // StringIndexerModel was not for a feature column, hence it must be for the target column when
                    // doing classification.
                    metaData.withNominalTargetValueMappings(Arrays.asList(stringIndexer.labelsArray()[0]));
                }
            }
        }

        // collect feature values using 'col=value' or 'col' as values
        final ArrayList<String> featureValues = new ArrayList<>();
        for (final String vecInputCol : vectorAssembler.getInputCols()) {
            final StringIndexerModel stringIndexer = oneHotStringIndexer.get(vecInputCol);
            if (stringIndexer != null) {
                for (final String label : stringIndexer.labelsArray()[0]) {
                    featureValues.add(stringIndexer.getInputCol() + "=" + label);
                }
                if (onHotEncoderHandleInvalid.equalsIgnoreCase("keep")) {
                    featureValues.add(stringIndexer.getInputCol() + "=?");
                }
            } else {
                featureValues.add(vecInputCol);
            }
        }

        metaData.withNominalFeatureValues(featureValues);
    }

}
