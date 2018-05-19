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
 *   Created on 27.04.2016 by koetter
 */
package org.knime.bigdata.spark1_3.base;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark1_3.api.Spark_1_3_CompatibilityChecker;
import org.knime.bigdata.spark1_3.jobs.fetchrows.FetchRowsJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.hive.Hive2SparkJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.hive.Spark2HiveJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.clustering.kmeans.KMeansJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.collaborativefiltering.CollaborativeFilteringJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.prediction.bayes.naive.NaiveBayesJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.prediction.decisiontree.DecisionTreeJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.prediction.ensemble.gradientboostedtrees.GradientBoostedTreesJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.prediction.ensemble.randomforest.RandomForestJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.prediction.linear.logistic.LogisticRegressionJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.prediction.linear.regression.LinearRegressionJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.prediction.linear.svm.SVMJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.prediction.predictor.PredictorSparkJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.reduction.pca.PCAJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.mllib.reduction.svd.SVDJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.namedobjects.NamedObjectsJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.pmml.PMMLPredictionJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.pmml.PMMLTransformationJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.prepare.PrepareContextJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.concatenate.ConcatenateRDDsJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.convert.category2number.Category2NumberJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.convert.number2category.Number2CategoryJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.filter.column.ColumnFilterJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.joiner.JoinJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.normalize.NormalizeColumnsJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.partition.PartitionJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.rename.RenameColumnJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.sampling.SamplingJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.preproc.sorter.SortJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.scorer.ClassificationScorerJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.scorer.EntropyScorerJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.scorer.ScorerJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.scripting.java.JavaSnippetJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.sql.SparkSQLJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.statistics.compute.StatisticsJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.statistics.correlation.CorrelationColumnJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.statistics.correlation.CorrelationMatrixJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.table2spark.Table2SparkJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.util.CheckJarPresenceJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.util.PersistJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.util.UnpersistJobRunFactory;
import org.knime.bigdata.spark1_3.jobs.util.UploadJarJobRunFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_1_3_JobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /**
     * Constructor.
     */
    public Spark_1_3_JobRunFactoryProvider() {
        super(Spark_1_3_CompatibilityChecker.INSTANCE,
            new FetchRowsJobRunFactory(),
            new Hive2SparkJobRunFactory(),
            new Spark2HiveJobRunFactory(),
            new KMeansJobRunFactory(),
            new CollaborativeFilteringJobRunFactory(),
            new NaiveBayesJobRunFactory(),
            new DecisionTreeJobRunFactory(),
            new GradientBoostedTreesJobRunFactory(),
            new RandomForestJobRunFactory(),
            new LinearRegressionJobRunFactory(),
            new LogisticRegressionJobRunFactory(),
            new SVMJobRunFactory(),
            new PredictorSparkJobRunFactory(),
            new PCAJobRunFactory(),
            new SVDJobRunFactory(),
            new NamedObjectsJobRunFactory(),
            new PrepareContextJobRunFactory(),
            new ConcatenateRDDsJobRunFactory(),
            new Category2NumberJobRunFactory(),
            new Number2CategoryJobRunFactory(),
            new ColumnFilterJobRunFactory(),
            new JoinJobRunFactory(),
            new NormalizeColumnsJobRunFactory(),
            new PartitionJobRunFactory(),
            new RenameColumnJobRunFactory(),
            new SamplingJobRunFactory(),
            new SortJobRunFactory(),
//            new TFIDFJobRunFactory(),
            new JavaSnippetJobRunFactory(),
            new Table2SparkJobRunFactory(),
            new StatisticsJobRunFactory(),
            new CorrelationColumnJobRunFactory(),
            new CorrelationMatrixJobRunFactory(),
            new PMMLPredictionJobRunFactory(),
            new PMMLTransformationJobRunFactory(),
            new ClassificationScorerJobRunFactory(),
            new ScorerJobRunFactory(),
            new EntropyScorerJobRunFactory(),
            new PersistJobRunFactory(),
            new UnpersistJobRunFactory(),
            new SparkSQLJobRunFactory(),
            new CheckJarPresenceJobRunFactory(),
            new UploadJarJobRunFactory());
    }
}
