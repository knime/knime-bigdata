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
package com.knime.bigdata.spark.node;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import com.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import com.knime.bigdata.spark.node.io.database.reader.Database2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.database.writer.Spark2DatabaseNodeFactory;
import com.knime.bigdata.spark.node.io.hive.reader.Hive2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.hive.writer.Spark2HiveNodeFactory;
import com.knime.bigdata.spark.node.io.impala.reader.Impala2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.impala.writer.Spark2ImpalaNodeFactory;
import com.knime.bigdata.spark.node.io.parquet.reader.Parquet2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.parquet.writer.Spark2ParquetNodeFactory;
import com.knime.bigdata.spark.node.io.table.reader.Table2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.table.writer.Spark2TableNodeFactory;
import com.knime.bigdata.spark.node.mllib.clustering.assigner.MLlibClusterAssignerNodeFactory;
import com.knime.bigdata.spark.node.mllib.clustering.kmeans.MLlibKMeansNodeFactory;
import com.knime.bigdata.spark.node.mllib.collaborativefiltering.MLlibCollaborativeFilteringNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.bayes.naive.MLlibNaiveBayesNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees.MLlibGradientBoostedTreeNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest.MLlibRandomForestNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.linear.logisticregression.MLlibLogisticRegressionNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.linear.regression.MLlibLinearRegressionNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.linear.svm.MLlibSVMNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.predictor.MLlibPredictorNodeFactory;
import com.knime.bigdata.spark.node.mllib.reduction.pca.MLlibPCANodeFactory;
import com.knime.bigdata.spark.node.mllib.reduction.svd.MLlibSVDNodeFactory;
import com.knime.bigdata.spark.node.pmml.converter.MLlib2PMMLNodeFactory;
import com.knime.bigdata.spark.node.pmml.predictor.compiled.SparkPMMLPredictorNodeFactory;
import com.knime.bigdata.spark.node.pmml.predictor.compiling.SparkPMMLCompilingPredictorNodeFactory;
import com.knime.bigdata.spark.node.pmml.transformation.compiled.SparkCompiledTransformationPMMLApplyNodeFactory;
import com.knime.bigdata.spark.node.pmml.transformation.compiling.SparkTransformationPMMLApplyNodeFactory;
import com.knime.bigdata.spark.node.preproc.concatenate.SparkConcatenateNodeFactory;
import com.knime.bigdata.spark.node.preproc.convert.category2number.SparkCategory2NumberNodeFactory;
import com.knime.bigdata.spark.node.preproc.convert.number2category.SparkNumber2CategoryNodeFactory;
import com.knime.bigdata.spark.node.preproc.filter.column.SparkColumnFilterNodeFactory;
import com.knime.bigdata.spark.node.preproc.joiner.SparkJoinerNodeFactory;
import com.knime.bigdata.spark.node.preproc.normalize.SparkNormalizerPMMLNodeFactory;
import com.knime.bigdata.spark.node.preproc.partition.SparkPartitionNodeFactory;
import com.knime.bigdata.spark.node.preproc.rename.SparkRenameColumnNodeFactory;
import com.knime.bigdata.spark.node.preproc.renameregex.SparkColumnRenameRegexNodeFactory;
import com.knime.bigdata.spark.node.preproc.sampling.SparkSamplingNodeFactory;
import com.knime.bigdata.spark.node.preproc.sorter.SparkSorterNodeFactory;
import com.knime.bigdata.spark.node.scorer.accuracy.SparkAccuracyScorerNodeFactory;
import com.knime.bigdata.spark.node.scorer.entropy.SparkEntropyScorerNodeFactory;
import com.knime.bigdata.spark.node.scorer.numeric.SparkNumericScorerNodeFactory;
import com.knime.bigdata.spark.node.scripting.java.sink.SparkJavaSnippetSinkNodeFactory;
import com.knime.bigdata.spark.node.scripting.java.snippet.SparkJavaSnippetNodeFactory;
import com.knime.bigdata.spark.node.scripting.java.source.SparkJavaSnippetSourceNodeFactory;
import com.knime.bigdata.spark.node.statistics.compute.MLlibStatisticsNodeFactory;
import com.knime.bigdata.spark.node.statistics.correlation.column.MLlibCorrelationColumnNodeFactory;
import com.knime.bigdata.spark.node.statistics.correlation.filter.MLlibCorrelationFilterNodeFactory;
import com.knime.bigdata.spark.node.statistics.correlation.matrix.MLlibCorrelationMatrixNodeFactory;
import com.knime.bigdata.spark.node.util.context.create.SparkContextCreatorNodeFactory;
import com.knime.bigdata.spark.node.util.context.destroy.SparkDestroyContextNodeFactory;
import com.knime.bigdata.spark.node.util.rdd.list.SparkListRDDNodeFactory;
import com.knime.bigdata.spark.node.util.rdd.persist.SparkPersistNodeFactory;
import com.knime.bigdata.spark.node.util.rdd.unpersist.SparkUnpersistNodeFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class StandardSparkNodeFactoryProvider extends DefaultSparkNodeFactoryProvider {

    /**
     * Constructor.
     */
    public StandardSparkNodeFactoryProvider() {
        super(AllVersionCompatibilityChecker.INSTANCE,
            new Database2SparkNodeFactory(),
            new Spark2DatabaseNodeFactory(),
            new Hive2SparkNodeFactory(),
            new Spark2HiveNodeFactory(),
            new Table2SparkNodeFactory(),
            new Spark2TableNodeFactory(),
            new Parquet2SparkNodeFactory(),
            new Spark2ParquetNodeFactory(),
            new MLlibClusterAssignerNodeFactory(),
            new MLlibKMeansNodeFactory(),
            new MLlibCollaborativeFilteringNodeFactory(),
            new MLlibNaiveBayesNodeFactory(),
            new MLlibDecisionTreeNodeFactory(),
            new MLlibGradientBoostedTreeNodeFactory(),
            new MLlibRandomForestNodeFactory(),
            new MLlibLogisticRegressionNodeFactory(),
            new MLlibLinearRegressionNodeFactory(),
            new MLlibSVMNodeFactory(),
            new MLlibPredictorNodeFactory(),
            new MLlibPCANodeFactory(),
            new MLlibSVDNodeFactory(),
            new SparkConcatenateNodeFactory(),
            new SparkCategory2NumberNodeFactory(),
            new SparkNumber2CategoryNodeFactory(),
            new SparkColumnFilterNodeFactory(),
            new SparkJoinerNodeFactory(),
            new SparkNormalizerPMMLNodeFactory(),
            new SparkPartitionNodeFactory(),
            new SparkRenameColumnNodeFactory(),
            new SparkColumnRenameRegexNodeFactory(),
            new SparkSamplingNodeFactory(),
            new SparkSorterNodeFactory(),
            new SparkJavaSnippetNodeFactory(),
            new SparkJavaSnippetSourceNodeFactory(),
            new SparkJavaSnippetSinkNodeFactory(),
            new MLlibStatisticsNodeFactory(),
            new MLlibCorrelationColumnNodeFactory(),
            new MLlibCorrelationMatrixNodeFactory(),
            new MLlibCorrelationFilterNodeFactory(),
            new SparkPersistNodeFactory(),
            new SparkUnpersistNodeFactory(),
            new SparkDestroyContextNodeFactory(),
            new SparkPMMLPredictorNodeFactory(),
            new SparkPMMLCompilingPredictorNodeFactory(),
            new SparkTransformationPMMLApplyNodeFactory(),
            new SparkCompiledTransformationPMMLApplyNodeFactory(),
            new MLlib2PMMLNodeFactory(),
            new SparkListRDDNodeFactory(),
            new SparkAccuracyScorerNodeFactory(),
            new SparkEntropyScorerNodeFactory(),
            new SparkNumericScorerNodeFactory(),
            new SparkContextCreatorNodeFactory(),
            new Spark2ImpalaNodeFactory(),
            new Impala2SparkNodeFactory());
    }

}
