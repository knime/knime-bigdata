package com.knime.bigdata.spark.node.scorer.entropy;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.knime.base.node.mine.scorer.entrop.EntropyCalculator;
import org.knime.core.data.RowKey;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.EntropyScorerJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.server.EntropyScorerData;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class EntropyScorerTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputRDD,
			final Integer aActualColumnIdx, final Integer aPredictionColumnIdx,
			final Boolean aIsClassification) throws GenericKnimeSparkException {

		return EntropyScorerTask.paramsAsJason(aInputRDD, aActualColumnIdx,
				aPredictionColumnIdx);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		final EntropyScorerTask testObj = new EntropyScorerTask(null, "inputRDD", 1, 5);
		final String params = testObj.learnerDef();
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new EntropyScorerJob().validate(config));
	}

	@Test
	public void computeScoresForPerfectCluster() throws Throwable {

		Object[][] clusters = new Object[][] { { 1, 0d, 0d }, { 2, 0d, 0d },
				{ 3, 1d, 1d }, { 4, 1d, 1d }, { 5, 2d, 2d }, { 6, 2d, 2d },
				{ 7, 3d, 3d }, { 8, 3d, 3d }, { 9, 4d, 4d }, { 10, 4d, 4d }, };

		Map<RowKey, RowKey> reference = new HashMap<>();
		Map<RowKey, Set<RowKey>> clusterMap = new HashMap<>();

		table2RowKeys(clusters, reference, clusterMap);

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, clusters, "tab1");
		final EntropyScorerTask testObj = new EntropyScorerTask(CONTEXT_ID, "tab1", 1, 2);

		EntropyScorerData entropyScore = (EntropyScorerData) testObj.execute(null);
		assertEquals("number of reference cluster, ", 5,
				entropyScore.getNrReferenceClusters());
		assertEquals("number of computed cluster, ", 5,
				entropyScore.getNrClusters());
		assertEquals("entropy, ",
				EntropyCalculator.getEntropy(reference, clusterMap),
				entropyScore.getOverallEntropy(), 0.00001d);
		assertEquals("quality, ", entropyScore.getOverallQuality(), entropyScore.getOverallQuality(), 0.00001d);
	}

	/**
	 * @param clusters
	 * @param reference
	 * @param clusterMap
	 */
	public static void table2RowKeys(Object[][] clusters,
			Map<RowKey, RowKey> reference, Map<RowKey, Set<RowKey>> clusterMap) {
		for(Object[] o : clusters) {
			reference.put(new RowKey(o[0].toString()), new RowKey(o[1].toString()));
			final RowKey key = new RowKey(o[2].toString());
			final Set<RowKey> clusterMembers;
			if (clusterMap.containsKey(key)) {
				clusterMembers = clusterMap.get(key);
			} else {
				clusterMembers = new HashSet<RowKey>();
				clusterMap.put(key, clusterMembers);
			}
			clusterMembers.add(new RowKey(o[0].toString()));
		}
	}

	
	@Test
	public void computeScoresForSomeCluster() throws Throwable {

		Object[][] clusters = new Object[][] { { 1, 0d, 0d }, { 2, 0d, 1d },
				{ 3, 1d, 3d }, { 4, 1d, 1d }, { 5, 2d, 2d }, { 6, 2d, 2d },
				{ 7, 3d, 3d }, { 8, 3d, 1d }, { 9, 4d, 0d }, { 10, 4d, 0d }, };

		Map<RowKey, RowKey> reference = new HashMap<>();
		Map<RowKey, Set<RowKey>> clusterMap = new HashMap<>();

		table2RowKeys(clusters, reference, clusterMap);

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, clusters, "tab1");
		final EntropyScorerTask testObj = new EntropyScorerTask(CONTEXT_ID, "tab1", 1, 2);

		EntropyScorerData entropyScore = (EntropyScorerData) testObj.execute(null);
		assertEquals("number of reference clusters, ", 5,
				entropyScore.getNrReferenceClusters());
		assertEquals("number of computed clusters, ", 4,
				entropyScore.getNrClusters());
		assertEquals("entropy, ",
				EntropyCalculator.getEntropy(reference, clusterMap),
				entropyScore.getOverallEntropy(), 0.00001d);
		assertEquals("quality, ", entropyScore.getOverallQuality(), entropyScore.getOverallQuality(), 0.00001d);
	}

	
}
