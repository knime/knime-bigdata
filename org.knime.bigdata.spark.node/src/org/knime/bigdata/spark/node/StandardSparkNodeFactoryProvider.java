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
package org.knime.bigdata.spark.node;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.node.io.database.db.reader.DB2SparkNodeFactory;
import org.knime.bigdata.spark.node.io.database.db.writer.Spark2DBNodeFactory;
import org.knime.bigdata.spark.node.io.database.hive.reader.DBHive2SparkNodeFactory;
import org.knime.bigdata.spark.node.io.database.hive.writer.DBSpark2HiveNodeFactory;
import org.knime.bigdata.spark.node.io.database.impala.reader.DBImpala2SparkNodeFactory;
import org.knime.bigdata.spark.node.io.database.impala.writer.DBSpark2ImpalaNodeFactory;
import org.knime.bigdata.spark.node.io.database.reader.Database2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.database.writer.Spark2DatabaseNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.avro.Avro2SparkNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.csv.CSV2SparkNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.json.Json2SparkNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.orc.Orc2SparkNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.parquet.Parquet2SparkNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.text.Text2SparkNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.avro.Spark2AvroNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.csv.Spark2CSVNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.json.Spark2JsonNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.orc.Spark2OrcNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.parquet.Spark2ParquetNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.text.Spark2TextNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.avro.Avro2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.csv.CSV2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.json.Json2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.orc.Orc2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.parquet.Parquet2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.text.Text2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.avro.Spark2AvroNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.csv.Spark2CSVNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.json.Spark2JsonNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.orc.Spark2OrcNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.parquet.Spark2ParquetNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.text.Spark2TextNodeFactory;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveNodeFactory;
import org.knime.bigdata.spark.node.io.impala.reader.Impala2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.impala.writer.Spark2ImpalaNodeFactory;
import org.knime.bigdata.spark.node.io.table.reader.Table2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.table.writer.Spark2TableNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeClassificationLearnerNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.regression.MLDecisionTreeRegressionLearnerNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.gbt.classification.MLGradientBoostedTreesClassificationLearnerNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.gbt.regression.MLGradientBoostedTreesRegressionLearnerNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.linear.regression.MLLinearRegressionLearnerNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.predictor.classification.MLPredictorClassificationNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.predictor.regression.MLPredictorRegressionNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.randomforest.classification.MLRandomForestClassificationLearnerNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.randomforest.regression.MLRandomForestRegressionLearnerNodeFactory;
import org.knime.bigdata.spark.node.mllib.associationrule.SparkAssociationRuleApplyNodeFactory;
import org.knime.bigdata.spark.node.mllib.associationrule.SparkAssociationRuleLearnerNodeFactory;
import org.knime.bigdata.spark.node.mllib.clustering.assigner.MLlibClusterAssignerNodeFactory;
import org.knime.bigdata.spark.node.mllib.clustering.kmeans.MLlibKMeansNodeFactory;
import org.knime.bigdata.spark.node.mllib.collaborativefiltering.MLlibCollaborativeFilteringNodeFactory;
import org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrequentItemSetNodeFactory;
import org.knime.bigdata.spark.node.mllib.prediction.bayes.naive.MLlibNaiveBayesNodeFactory;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeNodeFactory;
import org.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees.MLlibGradientBoostedTreeNodeFactory;
import org.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest.MLlibRandomForestNodeFactory;
import org.knime.bigdata.spark.node.mllib.prediction.linear.logisticregression.MLlibLogisticRegressionNodeFactory;
import org.knime.bigdata.spark.node.mllib.prediction.linear.regression.MLlibLinearRegressionNodeFactory;
import org.knime.bigdata.spark.node.mllib.prediction.linear.svm.MLlibSVMNodeFactory;
import org.knime.bigdata.spark.node.mllib.prediction.predictor.MLlibPredictorNodeFactory;
import org.knime.bigdata.spark.node.mllib.reduction.pca.MLlibPCANodeFactory;
import org.knime.bigdata.spark.node.mllib.reduction.pca.MLlibPCANodeFactory2;
import org.knime.bigdata.spark.node.mllib.reduction.svd.MLlibSVDNodeFactory;
import org.knime.bigdata.spark.node.pmml.converter.MLlib2PMMLNodeFactory;
import org.knime.bigdata.spark.node.pmml.predictor.compiled.SparkPMMLPredictorNodeFactory;
import org.knime.bigdata.spark.node.pmml.predictor.compiling.SparkPMMLCompilingPredictorNodeFactory;
import org.knime.bigdata.spark.node.pmml.transformation.compiled.SparkCompiledTransformationPMMLApplyNodeFactory;
import org.knime.bigdata.spark.node.pmml.transformation.compiling.SparkTransformationPMMLApplyNodeFactory;
import org.knime.bigdata.spark.node.preproc.concatenate.SparkConcatenateNodeFactory;
import org.knime.bigdata.spark.node.preproc.convert.category2number.SparkCategory2NumberNodeFactory;
import org.knime.bigdata.spark.node.preproc.convert.number2category.SparkNumber2CategoryNodeFactory;
import org.knime.bigdata.spark.node.preproc.filter.column.SparkColumnFilterNodeFactory;
import org.knime.bigdata.spark.node.preproc.filter.row.SparkRowFilterNodeFactory;
import org.knime.bigdata.spark.node.preproc.groupby.SparkGroupByNodeFactory;
import org.knime.bigdata.spark.node.preproc.joiner.SparkJoinerNodeFactory;
import org.knime.bigdata.spark.node.preproc.missingval.apply.SparkMissingValueApplyNodeFactory;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueNodeFactory;
import org.knime.bigdata.spark.node.preproc.normalize.SparkNormalizerPMMLNodeFactory;
import org.knime.bigdata.spark.node.preproc.partition.SparkPartitionNodeFactory;
import org.knime.bigdata.spark.node.preproc.pivot.SparkPivotNodeFactory;
import org.knime.bigdata.spark.node.preproc.rename.SparkRenameColumnNodeFactory;
import org.knime.bigdata.spark.node.preproc.renameregex.SparkColumnRenameRegexNodeFactory;
import org.knime.bigdata.spark.node.preproc.sampling.SparkSamplingNodeFactory;
import org.knime.bigdata.spark.node.preproc.sorter.SparkSorterNodeFactory;
import org.knime.bigdata.spark.node.scorer.accuracy.SparkAccuracyScorerNodeFactory;
import org.knime.bigdata.spark.node.scorer.entropy.SparkEntropyScorerNodeFactory;
import org.knime.bigdata.spark.node.scorer.numeric.SparkNumericScorerNodeFactory;
import org.knime.bigdata.spark.node.scripting.java.sink.SparkDataFrameJavaSnippetSinkNodeFactory;
import org.knime.bigdata.spark.node.scripting.java.sink.SparkJavaSnippetSinkNodeFactory;
import org.knime.bigdata.spark.node.scripting.java.snippet.SparkDataFrameJavaSnippetNodeFactory;
import org.knime.bigdata.spark.node.scripting.java.snippet.SparkJavaSnippetNodeFactory;
import org.knime.bigdata.spark.node.scripting.java.source.SparkDataFrameJavaSnippetSourceNodeFactory;
import org.knime.bigdata.spark.node.scripting.java.source.SparkJavaSnippetSourceNodeFactory2;
import org.knime.bigdata.spark.node.scripting.python.script1in1out.PySparkInner11NodeFactory;
import org.knime.bigdata.spark.node.scripting.python.script1in2out.PySparkInner12NodeFactory;
import org.knime.bigdata.spark.node.scripting.python.script2in1out.PySparkInner21NodeFactory;
import org.knime.bigdata.spark.node.scripting.python.script2in2out.PySparkInner22NodeFactory;
import org.knime.bigdata.spark.node.scripting.python.source.PySparkSourceNodeFactory;
import org.knime.bigdata.spark.node.sql.SparkSQLNodeFactory;
import org.knime.bigdata.spark.node.statistics.compute.MLlibStatisticsNodeFactory;
import org.knime.bigdata.spark.node.statistics.correlation.column.MLlibCorrelationColumnNodeFactory;
import org.knime.bigdata.spark.node.statistics.correlation.filter.MLlibCorrelationFilterNodeFactory;
import org.knime.bigdata.spark.node.statistics.correlation.matrix.MLlibCorrelationMatrixNodeFactory;
import org.knime.bigdata.spark.node.util.context.destroy.SparkDestroyContextNodeFactory;
import org.knime.bigdata.spark.node.util.rdd.list.SparkListRDDNodeFactory;
import org.knime.bigdata.spark.node.util.rdd.persist.SparkPersistNodeFactory;
import org.knime.bigdata.spark.node.util.rdd.unpersist.SparkUnpersistNodeFactory;
import org.knime.bigdata.spark.node.util.repartition.SparkRepartitionNodeFactory;

import com.knime.bigdata.spark.node.io.database.reader.Database2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.avro.Avro2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.csv.CSV2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.json.Json2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.orc.Orc2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.parquet.Parquet2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.text.Text2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.hive.reader.Hive2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.impala.reader.Impala2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.table.reader.Table2SparkNodeFactory;
import com.knime.bigdata.spark.node.scripting.java.source.SparkJavaSnippetSourceNodeFactory;

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
            // generic writer nodes
            new Avro2SparkNodeFactory3(),
            new CSV2SparkNodeFactory3(),
            new Json2SparkNodeFactory3(),
            new Orc2SparkNodeFactory3(),
            new Parquet2SparkNodeFactory3(),
            new Text2SparkNodeFactory3(),

            // generic writer nodes
            new Spark2AvroNodeFactory3(),
            new Spark2CSVNodeFactory3(),
            new Spark2JsonNodeFactory3(),
            new Spark2OrcNodeFactory3(),
            new Spark2ParquetNodeFactory3(),
            new Spark2TextNodeFactory3(),

            new Database2SparkNodeFactory2(),
            new DB2SparkNodeFactory(),
            new Spark2DatabaseNodeFactory(),
            new Spark2DBNodeFactory(),
            new Hive2SparkNodeFactory2(),
            new DBHive2SparkNodeFactory(),
            new DBImpala2SparkNodeFactory(),
            new Spark2HiveNodeFactory(),
            new DBSpark2HiveNodeFactory(),
            new DBSpark2ImpalaNodeFactory(),
            new Table2SparkNodeFactory2(),
            new Spark2TableNodeFactory(),
            new MLlibClusterAssignerNodeFactory(),
            new MLlibKMeansNodeFactory(),
            new MLlibCollaborativeFilteringNodeFactory(),
            new SparkFrequentItemSetNodeFactory(),
            new MLlibNaiveBayesNodeFactory(),
            new MLlibLogisticRegressionNodeFactory(),
            new MLlibLinearRegressionNodeFactory(),
            new MLlibSVMNodeFactory(),
            new SparkAssociationRuleApplyNodeFactory(),
            new SparkAssociationRuleLearnerNodeFactory(),
            new MLlibPredictorNodeFactory(),
            new MLlibPCANodeFactory2(),
            new MLlibSVDNodeFactory(),
            new SparkConcatenateNodeFactory(),
            new SparkCategory2NumberNodeFactory(),
            new SparkNumber2CategoryNodeFactory(),
            new SparkColumnFilterNodeFactory(),
            new SparkRowFilterNodeFactory(),
            new SparkGroupByNodeFactory(),
            new SparkPivotNodeFactory(),
            new SparkJoinerNodeFactory(),
            new SparkNormalizerPMMLNodeFactory(),
            new SparkPartitionNodeFactory(),
            new SparkRenameColumnNodeFactory(),
            new SparkColumnRenameRegexNodeFactory(),
            new SparkSamplingNodeFactory(),
            new SparkSorterNodeFactory(),
            new SparkSQLNodeFactory(),
            new SparkJavaSnippetNodeFactory(),
            new SparkJavaSnippetSourceNodeFactory2(),
            new SparkJavaSnippetSinkNodeFactory(),
            new SparkDataFrameJavaSnippetSourceNodeFactory(),
            new SparkDataFrameJavaSnippetNodeFactory(),
            new SparkDataFrameJavaSnippetSinkNodeFactory(),
            new MLlibStatisticsNodeFactory(),
            new MLlibCorrelationColumnNodeFactory(),
            new MLlibCorrelationMatrixNodeFactory(),
            new MLlibCorrelationFilterNodeFactory(),
            new SparkPersistNodeFactory(),
            new SparkUnpersistNodeFactory(),
            new SparkRepartitionNodeFactory(),
            new SparkDestroyContextNodeFactory(),
            new SparkMissingValueNodeFactory(),
            new SparkMissingValueApplyNodeFactory(),
            new SparkPMMLPredictorNodeFactory(),
            new SparkPMMLCompilingPredictorNodeFactory(),
            new SparkTransformationPMMLApplyNodeFactory(),
            new SparkCompiledTransformationPMMLApplyNodeFactory(),
            new MLlib2PMMLNodeFactory(),
            new SparkListRDDNodeFactory(),
            new SparkAccuracyScorerNodeFactory(),
            new SparkEntropyScorerNodeFactory(),
            new SparkNumericScorerNodeFactory(),
            new Spark2ImpalaNodeFactory(),
            new Impala2SparkNodeFactory2(),
            new PySparkInner11NodeFactory(),
            new PySparkInner22NodeFactory(),
            new PySparkInner21NodeFactory(),
            new PySparkInner12NodeFactory(),
            new PySparkSourceNodeFactory(),

            // deprecated generic spark writer nodes (connection information port)
            new Spark2AvroNodeFactory(),
            new Spark2CSVNodeFactory(),
            new Spark2JsonNodeFactory(),
            new Spark2OrcNodeFactory(),
            new Spark2ParquetNodeFactory(),
            new Spark2TextNodeFactory(),

            // deprecated generic spark reader nodes (connection information port)
            new Avro2SparkNodeFactory2(),
            new CSV2SparkNodeFactory2(),
            new Json2SparkNodeFactory2(),
            new Orc2SparkNodeFactory2(),
            new Parquet2SparkNodeFactory2(),
            new Text2SparkNodeFactory2(),

            // deprecated generic spark reader nodes (optional spark context port)
            new Avro2SparkNodeFactory(),
            new CSV2SparkNodeFactory(),
            new Json2SparkNodeFactory(),
            new Orc2SparkNodeFactory(),
            new Parquet2SparkNodeFactory(),

            // other deprecated nodes
            new Database2SparkNodeFactory(),
            new Hive2SparkNodeFactory(),
            new MLlibDecisionTreeNodeFactory(),
            new MLlibGradientBoostedTreeNodeFactory(),
            new MLlibRandomForestNodeFactory(),
            new Table2SparkNodeFactory(),
            new Text2SparkNodeFactory(),
            new Impala2SparkNodeFactory(),
            new SparkJavaSnippetSourceNodeFactory(),
            new MLlibPCANodeFactory(),

            // ML based learner and predicter nodes
            new MLDecisionTreeClassificationLearnerNodeFactory(),
            new MLDecisionTreeRegressionLearnerNodeFactory(),
            new MLLinearRegressionLearnerNodeFactory(),
            new MLRandomForestClassificationLearnerNodeFactory(),
            new MLRandomForestRegressionLearnerNodeFactory(),
            new MLGradientBoostedTreesClassificationLearnerNodeFactory(),
            new MLGradientBoostedTreesRegressionLearnerNodeFactory(),
            new MLPredictorClassificationNodeFactory(),
            new MLPredictorRegressionNodeFactory());
    }

}
