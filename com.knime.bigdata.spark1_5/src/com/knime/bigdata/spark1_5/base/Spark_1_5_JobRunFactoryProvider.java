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
 *   Created on 27.04.2016 by koetter
 */
package com.knime.bigdata.spark1_5.base;

import com.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import com.knime.bigdata.spark1_5.api.Spark_1_5_CompatibilityChecker;
import com.knime.bigdata.spark1_5.jobs.database.Database2SparkJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.database.Spark2DatabaseJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.fetchrows.FetchRowsJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.genericdatasource.GenericDataSource2SparkJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.genericdatasource.Spark2GenericDataSourceJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.hive.Hive2SparkJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.hive.Spark2HiveJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.clustering.kmeans.KMeansJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.collaborativefiltering.CollaborativeFilteringJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.bayes.naive.NaiveBayesJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.decisiontree.DecisionTreeJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.ensemble.gradientboostedtrees.GradientBoostedTreesJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.ensemble.randomforest.RandomForestJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.linear.logistic.LogisticRegressionJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.linear.regression.LinearRegressionJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.linear.svm.SVMJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.predictor.PredictorSparkJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.reduction.pca.PCAJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.reduction.svd.SVDJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.namedobjects.NamedObjectsJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.pmml.PMMLPredictionJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.pmml.PMMLTransformationJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.prepare.PrepareContextJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.concatenate.ConcatenateRDDsJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.convert.category2number.Category2NumberJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.convert.number2category.Number2CategoryJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.filter.column.ColumnFilterJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.joiner.JoinJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.normalize.NormalizeColumnsJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.partition.PartitionJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.rename.RenameColumnJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.sampling.SamplingJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.preproc.sorter.SortJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.scorer.ClassificationScorerJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.scorer.EntropyScorerJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.scorer.ScorerJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.scripting.java.JavaSnippetJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.sql.SparkSQLFunctionsJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.sql.SparkSQLJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.statistics.compute.StatisticsJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.statistics.correlation.CorrelationColumnJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.statistics.correlation.CorrelationMatrixJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.table2spark.Table2SparkJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.util.PersistJobRunFactory;
import com.knime.bigdata.spark1_5.jobs.util.UnpersistJobRunFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_1_5_JobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /**
     * Constructor.
     */
    public Spark_1_5_JobRunFactoryProvider() {
        super(Spark_1_5_CompatibilityChecker.INSTANCE,
            new FetchRowsJobRunFactory(),
            new Database2SparkJobRunFactory(),
            new Spark2DatabaseJobRunFactory(),
            new GenericDataSource2SparkJobRunFactory(),
            new Spark2GenericDataSourceJobRunFactory(),
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
            new SparkSQLFunctionsJobRunFactory());
    }
}
