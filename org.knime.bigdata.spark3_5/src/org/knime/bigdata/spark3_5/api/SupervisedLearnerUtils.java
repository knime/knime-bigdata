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
 *   Created on 27.07.2015 by dwk
 */
package org.knime.bigdata.spark3_5.api;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.job.ClassificationJobInput;
import org.knime.bigdata.spark.core.job.ClassificationWithNominalFeatureInfoJobInput;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author dwk
 */
@SparkClass
public class SupervisedLearnerUtils {
    /**
     * array with the indices of the nominal columns
     */
    public static final String PARAM_NOMINAL_FEATURE_INFO = "NominalFeatureInfo";

    /** Number of classes. **/
    public static final String PARAM_NO_OF_CLASSES = "NumberOfClasses";

    /**
     * @param input
     * @param dataset
     * @return LabeledPoint RDD with training data
     */
    public static JavaRDD<LabeledPoint> getTrainingData(final ClassificationJobInput input, final Dataset<Row> dataset) {
        final List<Integer> colIdxs = input.getColumnIdxs();
        //note: requires that all features (including the label) are numeric !!!
        final Integer labelIndex = input.getClassColIdx();
        final JavaRDD<LabeledPoint> inputRdd = RDDUtilsInJava.toLabeledPointRDD(dataset, colIdxs, labelIndex);
        return inputRdd;
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
        final JobInput input, final JavaRDD<Row> rowRDD, final JavaRDD<LabeledPoint> inputRdd,
        final Serializable model) throws Exception {
        storePredictions(sparkContext, input, namedObjects, rowRDD, toVectorRDDFromLabeledPointRDD(inputRdd), model);
    }

    /**
     * Extracts features from {@link LabeledPoint}s.
     *
     * @param inputRdd RDD of labeled points
     * @return features of labeled points
     */
    private static JavaRDD<Vector> toVectorRDDFromLabeledPointRDD(final JavaRDD<LabeledPoint> inputRdd) {
        return inputRdd.map(new Function<LabeledPoint, Vector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Vector call(final LabeledPoint point) {
                return point.features();
            }
        });
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
            final JavaRDD<Row> aInputRdd, final JavaRDD<Vector> aFeatures, final Serializable aModel)
            throws Exception {

        if (!input.getNamedOutputObjects().isEmpty()) {
            final SparkSession spark = SparkSession.builder().sparkContext(sc).getOrCreate();
            final String namedOutputObject = input.getFirstNamedOutputObject();
            //TODO - revert the label to int mapping ????
            final JavaRDD<Row> predictedData = ModelUtils.predict(aFeatures, aInputRdd, aModel);
            final StructType schema = TypeConverters.convertSpec(input.getSpec(namedOutputObject));
            final Dataset<Row> predictedDataset = spark.createDataFrame(predictedData, schema);
            namedObjects.addDataFrame(namedOutputObject , predictedDataset);
        }
    }

    /**
     * compute the number of classes (or distinct values of a feature)
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
     * @param aInputData the input named object that contains the classification column to get the unique
     * values from if it is not present in the job input object
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
