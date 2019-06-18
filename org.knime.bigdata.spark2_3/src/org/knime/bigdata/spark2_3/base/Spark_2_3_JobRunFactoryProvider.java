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
package org.knime.bigdata.spark2_3.base;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark2_3.api.Spark_2_3_CompatibilityChecker;
import org.knime.bigdata.spark2_3.jobs.database.Database2SparkJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.database.Spark2DatabaseJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.fetchrows.FetchRowsJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.genericdatasource.GenericDataSource2SparkJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.genericdatasource.Spark2GenericDataSourceJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.hive.Hive2SparkJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.hive.Spark2HiveJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.ml.prediction.decisiontree.classification.MLDecisionTreeClassificationLearnerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.ml.prediction.decisiontree.regression.MLDecisionTreeRegressionLearnerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.ml.prediction.gbt.classification.MLGradientBoostedTreesClassificationLearnerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.ml.prediction.gbt.regression.MLGradientBoostedTreesRegressionLearnerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.ml.prediction.randomforest.classification.MLRandomForestClassificationLearnerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.ml.prediction.randomforest.regression.MLRandomForestRegressionLearnerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.ml.predictor.classification.MLPredictorClassificationJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.associationrule.AssociationRuleApplyJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.associationrule.AssociationRuleLearnerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.clustering.kmeans.KMeansJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.collaborativefiltering.CollaborativeFilteringJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.freqitemset.FrequentItemSetJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.prediction.bayes.naive.NaiveBayesJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.prediction.decisiontree.DecisionTreeJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.prediction.ensemble.gradientboostedtrees.GradientBoostedTreesJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.prediction.ensemble.randomforest.RandomForestJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.prediction.linear.logistic.LogisticRegressionJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.prediction.linear.regression.LinearRegressionJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.prediction.linear.svm.SVMJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.prediction.predictor.PredictorSparkJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.reduction.pca.PCAJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.mllib.reduction.svd.SVDJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.namedmodels.NamedModelCheckerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.namedmodels.NamedModelUploaderJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.namedobjects.NamedObjectsJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.pmml.PMMLPredictionJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.pmml.PMMLTransformationJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.prepare.PrepareContextJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.prepare.livy.LivyPrepareContextJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.concatenate.ConcatenateDataFramesJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.convert.category2number.Category2NumberJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.convert.number2category.Number2CategoryJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.filter.column.ColumnFilterJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.groupby.GroupByJobFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.joiner.JoinJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.missingval.MissingValueJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.normalize.NormalizeColumnsJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.partition.PartitionJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.rename.RenameColumnJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.sampling.SamplingJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.preproc.sorter.SortJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.scorer.AccuracyScorerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.scorer.EntropyScorerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.scorer.NumericScorerJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.scripting.java.JavaDataFrameSnippetJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.scripting.java.JavaSnippetJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.scripting.python.PySparkJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.sql.SparkSQLFunctionsJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.sql.SparkSQLJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.statistics.compute.StatisticsJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.statistics.correlation.CorrelationColumnJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.statistics.correlation.CorrelationMatrixJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.table2spark.Table2SparkJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.util.CheckJarPresenceJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.util.PersistJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.util.RepartitionJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.util.UnpersistJobRunFactory;
import org.knime.bigdata.spark2_3.jobs.util.UploadJarJobRunFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_2_3_JobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /**
     * Constructor.
     */
    public Spark_2_3_JobRunFactoryProvider() {
        super(Spark_2_3_CompatibilityChecker.INSTANCE,
            new Database2SparkJobRunFactory(),
            new Spark2DatabaseJobRunFactory(),
            new FetchRowsJobRunFactory(),
            new GenericDataSource2SparkJobRunFactory(),
            new Spark2GenericDataSourceJobRunFactory(),
            new Hive2SparkJobRunFactory(),
            new Spark2HiveJobRunFactory(),
            new KMeansJobRunFactory(),
            new CollaborativeFilteringJobRunFactory(),
            new FrequentItemSetJobRunFactory(),
            new AssociationRuleApplyJobRunFactory(),
            new AssociationRuleLearnerJobRunFactory(),
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
            new LivyPrepareContextJobRunFactory(),
            new ConcatenateDataFramesJobRunFactory(),
            new Category2NumberJobRunFactory(),
            new Number2CategoryJobRunFactory(),
            new ColumnFilterJobRunFactory(),
            new GroupByJobFactory(),
            new JoinJobRunFactory(),
            new MissingValueJobRunFactory(),
            new NormalizeColumnsJobRunFactory(),
            new RenameColumnJobRunFactory(),
            new PartitionJobRunFactory(),
            new SamplingJobRunFactory(),
            new SortJobRunFactory(),
//            new TFIDFJobRunFactory(),
            new JavaSnippetJobRunFactory(),
            new JavaDataFrameSnippetJobRunFactory(),
            new Table2SparkJobRunFactory(),
            new StatisticsJobRunFactory(),
            new CorrelationColumnJobRunFactory(),
            new CorrelationMatrixJobRunFactory(),
            new PMMLPredictionJobRunFactory(),
            new PMMLTransformationJobRunFactory(),
            new AccuracyScorerJobRunFactory(),
            new NumericScorerJobRunFactory(),
            new EntropyScorerJobRunFactory(),
            new PersistJobRunFactory(),
            new UnpersistJobRunFactory(),
            new RepartitionJobRunFactory(),
            new SparkSQLJobRunFactory(),
            new SparkSQLFunctionsJobRunFactory(),
            new CheckJarPresenceJobRunFactory(),
            new UploadJarJobRunFactory(),
            new PySparkJobRunFactory(),
            new MLDecisionTreeClassificationLearnerJobRunFactory(),
            new MLDecisionTreeRegressionLearnerJobRunFactory(),
            new MLGradientBoostedTreesClassificationLearnerJobRunFactory(),
            new MLGradientBoostedTreesRegressionLearnerJobRunFactory(),
            new MLRandomForestClassificationLearnerJobRunFactory(),
            new MLRandomForestRegressionLearnerJobRunFactory(),
            new NamedModelCheckerJobRunFactory(),
            new NamedModelUploaderJobRunFactory(),
            new MLPredictorClassificationJobRunFactory());
    }
}
