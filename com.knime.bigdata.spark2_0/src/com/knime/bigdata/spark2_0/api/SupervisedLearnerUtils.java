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
 *   Created on 27.07.2015 by dwk
 */
package com.knime.bigdata.spark2_0.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.PredictionModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.feature.ColumnPruner;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.ClassificationJobInput;
import com.knime.bigdata.spark.core.job.ClassificationWithNominalFeatureInfoJobInput;
import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;

import scala.collection.immutable.Set;

/**
 *
 * @author dwk
 */
@SparkClass
public class SupervisedLearnerUtils {

    private final static Logger LOGGER = Logger.getLogger(SupervisedLearnerUtils.class.getName());

    /**
     * array with the indices of the nominal columns
     */
    public static final String PARAM_NOMINAL_FEATURE_INFO = "NominalFeatureInfo";

    /** Number of classes. **/
    public static final String PARAM_NO_OF_CLASSES = "NumberOfClasses";

    /**
     * @param input
     * @param aRowRDD
     * @return LabeledPoint RDD with training data
     */
    public static JavaRDD<LabeledPoint> getTrainingData(final ClassificationJobInput input,
        final JavaRDD<Row> aRowRDD) {
        final List<Integer> colIdxs = input.getColumnIdxs();
        //note: requires that all features (including the label) are numeric !!!
        final Integer labelIndex = input.getClassColIdx();
        final JavaRDD<LabeledPoint> inputRdd = RDDUtilsInJava.toJavaLabeledPointRDD(aRowRDD, colIdxs, labelIndex);
        return inputRdd;
    }

    /**
     *
     * @param aSparkContext spark context
     * @param aConfigurationInput configuration of the classification job
     * @param aUnprocessedTrainingData - data to fit model to
     * @param aConfiguredClassifier - a pre-configured classifier as part of the training pipeline
     * @param aIndexLabelColumn convert label column to a (numeric) index, not required for regression tasks
     * @return ModelJobOutput the wrapped PipelineModel
     * @throws KNIMESparkException
     */
    public static <FeaturesType, Learner extends Predictor<FeaturesType, Learner, M>, M extends PredictionModel<FeaturesType, M>>
        ModelJobOutput constructAndExecutePipeline(final SparkContext aSparkContext,
            final ClassificationWithNominalFeatureInfoJobInput aConfigurationInput,
            final Dataset<Row> aUnprocessedTrainingData,
            final Predictor<FeaturesType, Learner, M> aConfiguredClassifier, final boolean aIndexLabelColumn)
            throws KNIMESparkException {

        final List<PipelineStage> pipelineStages = new ArrayList<>();

        final Integer labelIndex = aConfigurationInput.getClassColIdx();
        final String labelColumnName = aUnprocessedTrainingData.columns()[labelIndex];

        final List<String> featureNames = new ArrayList<>();
        for (String col : aConfigurationInput.getColumnNames(aUnprocessedTrainingData.columns())) {
            featureNames.add(col);
        }

        final String featureColumn = ModelUtils.getTemporaryColumnName("features");

        // Automatically identify categorical features, and index them - needs to be done before vector is created
        // key: nominal feature index, value: number of distinct values
        final Map<Integer, Integer> nominalFeatureInfo = aConfigurationInput.getNominalFeatureInfo().getMap();
        if (nominalFeatureInfo.size() > 0) {
            LOGGER.log(Level.INFO,
                "Adding string indexers for " + nominalFeatureInfo.size() + " nominal features to pipeline.");
            System.err
                .println("Adding string indexers for " + nominalFeatureInfo.size() + " nominal features to pipeline.");

            for (Integer nominalFeatureIndex : nominalFeatureInfo.keySet()) {
                String nominalColName = featureNames.get(nominalFeatureIndex);
                if (aConfigurationInput.getUseOneHotEncoding()) {
                    featureNames.remove(nominalFeatureIndex);
                    featureNames.addAll(addToPipelineAsOneHotEncodingMapping(nominalFeatureIndex, nominalColName, true,
                        null, pipelineStages, aUnprocessedTrainingData));
                } else {
                    String newNominalColName = nominalColName + "_i";
                    pipelineStages.add(new StringIndexer().setInputCol(nominalColName).setOutputCol(newNominalColName)
                        .fit(aUnprocessedTrainingData));
                    // overwrite name so that vector assembly processes the correct column:
                    featureNames.set(nominalFeatureIndex, newNominalColName);
                }
            }

        }

        //TODO - column names must not contain special chars like '(', ')', or '_'
        final VectorAssembler va = new VectorAssembler()
            .setInputCols(featureNames.toArray(new String[featureNames.size()])).setOutputCol(featureColumn);
        pipelineStages.add(va);

        final String tmpClassColumn;
        final StringIndexerModel labelIndexer;
        if (aIndexLabelColumn) {
            tmpClassColumn = ModelUtils.getTemporaryColumnName("label");
            // Index labels, adding metadata to the label column.
            labelIndexer = new StringIndexer().setInputCol(labelColumnName).setOutputCol(tmpClassColumn)
                .fit(aUnprocessedTrainingData);
            pipelineStages.add(labelIndexer);
        } else {
            tmpClassColumn = labelColumnName;
            labelIndexer = null;
        }

        LOGGER.log(Level.INFO, "Constructing classification training pipeline");

        pipelineStages.add(aConfiguredClassifier.setLabelCol(tmpClassColumn).setFeaturesCol(featureColumn));

        // appears not to have any effect: .setPredictionCol("colp")

        if (labelIndexer != null) {
            // Convert indexed labels back to original labels.
            pipelineStages.add(new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels()));
        }

        pipelineStages.add(new ColumnPruner(new Set.Set1<String>(featureColumn)));

        // Chain indexers and classifier in a Pipeline.
        Pipeline pipeline = new Pipeline().setStages(pipelineStages.toArray(new PipelineStage[pipelineStages.size()]));

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(aUnprocessedTrainingData);
        return new ModelJobOutput(model);
    }

    /**
     * @param dataset - the dataset to be processed
     * @param pipelineStages - existing pipeline stages, new stages will be added
     * @param indexers - optional map of indexers
     * @param nominalColName - name of column to be converted
     * @param colIndex - index of column to be converted
     * @param aDropLast - drop last value (if true then a column with N values will be mapped onto N-1 boolean columns,
     *            all 0 indicates then implicitly the last value)
     * @return list of added (temporary) column names
     *
     */
    public static List<String> addToPipelineAsOneHotEncodingMapping(final Integer colIndex, final String nominalColName,
        final boolean aDropLast, final Map<Integer, StringIndexerModel> indexers,
        final List<PipelineStage> pipelineStages, final Dataset<Row> dataset) {

        final String stringIndexerOutputColumn = ModelUtils.getTemporaryColumnName("feature");

        final StringIndexerModel indexer =
            new StringIndexer().setInputCol(nominalColName).setOutputCol(stringIndexerOutputColumn).fit(dataset);
        pipelineStages.add(indexer);
        if (indexers != null) {
            indexers.put(colIndex, indexer);
        }

        final String oneHotOutputColumn = ModelUtils.getTemporaryColumnName("feature");
        //use one-hot encoding
        OneHotEncoder oneHotEncoder = new OneHotEncoder().setInputCol(stringIndexerOutputColumn)
            .setOutputCol(oneHotOutputColumn).setDropLast(aDropLast);
        pipelineStages.add(oneHotEncoder);
        pipelineStages
            .add(new ColumnPruner(new scala.collection.immutable.Set.Set1<String>(stringIndexerOutputColumn)));

        //OneHotEncoder produces a column with a Vector
        // as of Spark ??? there exists a VectorDisassembler, here we use a local copy
        VectorDisassembler df = new VectorDisassembler();
        df.setInputCol(oneHotOutputColumn);
        pipelineStages.add(df);
        pipelineStages.add(new ColumnPruner(new scala.collection.immutable.Set.Set1<String>(oneHotOutputColumn)));

        final List<String> appendedColumnNames = new ArrayList<>();
        //target columns are now, for each value:  nominalColName + "_" + value
        final String[] labels = indexer.labels();
        for (int ix = 0; ix < labels.length; ix++) {
            if (!aDropLast || ix < labels.length - 1) {
                appendedColumnNames.add(nominalColName + "_" + labels[ix]);
            }
        }
        return appendedColumnNames;
    }

    /**
     * @param sparkContext
     * @param input
     * @param namedObjects
     * @param rowRDD
     * @param inputRdd
     * @param model
     * @throws Exception
     */
    public static void storePredictions(final SparkContext sparkContext, final NamedObjects namedObjects,
        final JobInput input, final JavaRDD<Row> rowRDD, final JavaRDD<LabeledPoint> inputRdd, final Serializable model)
        throws Exception {
        storePredictions(sparkContext, input, namedObjects, rowRDD, RDDUtils.toVectorRDDFromLabeledPointRDD(inputRdd),
            model);
    }

    /**
     * @param sc
     * @param input
     * @param namedObjects
     * @param aInputRdd
     * @param aFeatures
     * @param aModel
     * @throws Exception
     */
    public static void storePredictions(final SparkContext sc, final JobInput input, final NamedObjects namedObjects,
        final JavaRDD<Row> aInputRdd, final JavaRDD<Vector> aFeatures, final Serializable aModel) throws Exception {

        if (!input.getNamedOutputObjects().isEmpty()) {
            final SparkSession spark = SparkSession.builder().sparkContext(sc).getOrCreate();
            final String namedOutputObject = input.getFirstNamedOutputObject();
            //TODO - revert the label to int mapping ????
            final JavaRDD<Row> predictedData = ModelUtils.predict(aFeatures, aInputRdd, aModel);
            final StructType schema = TypeConverters.convertSpec(input.getSpec(namedOutputObject));
            final Dataset<Row> predictedDataset = spark.createDataFrame(predictedData, schema);
            namedObjects.addDataFrame(namedOutputObject, predictedDataset);
        }
    }

    /**
     * compute the number of classes (or distinct values of a feature)
     *
     * @param aRDD
     * @param aColumn
     * @return the number of distinct values for the given column index
     */
    public static long getNumberValuesOfColumn(final JavaRDD<Row> aRDD, final int aColumn) {
        return aRDD.map(new Function<Row, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object call(final Row aRow) throws Exception {
                return aRow.get(aColumn);
            }
        }).distinct().count();
    }

    /**
     * find the distinct values of a column
     *
     * @param aRDD
     * @param aColumn
     * @return the distinct values for the given column index
     */
    public static JavaRDD<Object> getDistinctValuesOfColumn(final JavaRDD<Row> aRDD, final int aColumn) {
        return aRDD.map(new Function<Row, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object call(final Row aRow) throws Exception {
                return aRow.get(aColumn);
            }
        }).distinct();
    }

    /**
     * compute the number of classes
     *
     * @param aRDD
     * @return the number of distinct labels
     */
    public static long getNumberOfLabels(final JavaRDD<LabeledPoint> aRDD) {
        return aRDD.map(new Function<LabeledPoint, Double>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double call(final LabeledPoint aPoint) throws Exception {
                return aPoint.label();
            }
        }).distinct().count();
    }

    /**
     * @param input {@link ClassificationWithNominalFeatureInfoJobInput} to get the number of classes from
     * @param aInputData the input named object that contains the classification column to get the unique values from if
     *            it is not present in the job input object
     * @return the number of unique values in the classification column
     */
    public static Long getNoOfClasses(final ClassificationWithNominalFeatureInfoJobInput input,
        final JavaRDD<LabeledPoint> aInputData) {
        final Map<Integer, Integer> nominalFeatureInfo = input.getNominalFeatureInfo().getMap();
        final Integer labelIndex = input.getClassColIdx();
        final Long numClasses;
        if (input.getNoOfClasses() != null) {
            numClasses = input.getNoOfClasses();
        } else if (nominalFeatureInfo.containsKey(labelIndex)) {
            numClasses = nominalFeatureInfo.get(labelIndex).longValue();
        } else {
            //Get number of classes from the input data
            numClasses = getNumberOfLabels(aInputData);
        }
        return numClasses;
    }
}
