package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.knime.core.data.RowKey;
import org.knime.core.util.MutableInteger;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.scorer.entropy.EntropyScorerTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class EntropyScorerJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = EntropyScorerTaskTest.paramsAsJason(null, 1, 2, true);
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_ACTUAL_COL_INDEX_Parameter()
			throws Throwable {
		String params = EntropyScorerTaskTest
				.paramsAsJason("in", null, 2, true);
		myCheck(params, EntropyScorerJob.PARAM_ACTUAL_COL_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PRED_COL_INDEX_Parameter()
			throws Throwable {
		String params = EntropyScorerTaskTest
				.paramsAsJason("in", 1, null, true);
		myCheck(params, EntropyScorerJob.PARAM_PREDICTION_COL_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = EntropyScorerTaskTest.paramsAsJason("in", 1, 2, true);
		KnimeSparkJob testObj = new EntropyScorerJob();
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(), testObj.validate(config2));
	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new EntropyScorerJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

	@Test
	public void entropyOfTrivialClusterShouldBe0() throws Throwable {
		Map<Object, Integer> cluster = new HashMap<>();
		cluster.put(0, 3);
		Map<RowKey, RowKey> ref = new HashMap<>();

		ref.put(new RowKey("1"), new RowKey("0"));
		ref.put(new RowKey("2"), new RowKey("0"));
		ref.put(new RowKey("3"), new RowKey("0"));
		final Set<RowKey> clusterMembers = new HashSet<RowKey>();
		clusterMembers.add(new RowKey("1"));
		clusterMembers.add(new RowKey("2"));
		clusterMembers.add(new RowKey("3"));

		final double e = entropy(ref, clusterMembers);
		assertEquals("trivial cluster", e,
				EntropyScorerJob.clusterEntropy(cluster, 3), 0.000001d);
	}

	@Test
	public void entropyOfNonTrivialCluster() throws Throwable {
		Map<Object, Integer> cluster = new HashMap<>();
		cluster.put(0, 3);
		cluster.put(1, 1);
		cluster.put(2, 1);
		Map<RowKey, RowKey> ref = new HashMap<>();

		ref.put(new RowKey("1"), new RowKey("0"));
		ref.put(new RowKey("2"), new RowKey("0"));
		ref.put(new RowKey("3"), new RowKey("1"));
		ref.put(new RowKey("4"), new RowKey("0"));
		ref.put(new RowKey("5"), new RowKey("2"));
		ref.put(new RowKey("6"), new RowKey("1"));
		final Set<RowKey> clusterMembers = new HashSet<RowKey>();
		clusterMembers.add(new RowKey("1"));
		clusterMembers.add(new RowKey("2"));
		clusterMembers.add(new RowKey("3"));
		clusterMembers.add(new RowKey("4"));
		clusterMembers.add(new RowKey("5"));

		final double e = entropy(ref, clusterMembers);
		assertEquals("trivial cluster", e,
				EntropyScorerJob.clusterEntropy(cluster, 5), 0.000001d);
	}


	static double getEntropy(final Map<RowKey, RowKey> reference,
			final Map<RowKey, Set<RowKey>> clusterMap) {
		// optimistic guess (we don't have counterexamples!)
		if (clusterMap.isEmpty()) {
			return 0.0;
		}
		double entropy = 0.0;
		int patCount = 0;
		for (Set<RowKey> pats : clusterMap.values()) {
			int size = pats.size();
			patCount += size;
			double e = entropy(reference, pats);
			entropy += size * e;
		}
		// normalizing over the number of objects in the reference set
		return entropy / patCount;
	}

	static double entropy(final Map<RowKey, RowKey> ref, final Set<RowKey> pats) {
		// that will map the "original" cluster ID to a counter.
		HashMap<RowKey, MutableInteger> refClusID2Count = new HashMap<RowKey, MutableInteger>();
		for (RowKey pat : pats) {
			RowKey origCluster = ref.get(pat);
			MutableInteger countForClus = refClusID2Count.get(origCluster);
			// if we haven't had cluster id before ...
			if (countForClus == null) {
				// init the counter with 1
				refClusID2Count.put(origCluster, new MutableInteger(1));
			} else {
				countForClus.inc();
			}
		}
		final int size = pats.size();
		double e = 0.0;
		for (MutableInteger clusterCount : refClusID2Count.values()) {
			int count = clusterCount.intValue();
			double quot = count / (double) size;
			e -= quot * Math.log(quot) / Math.log(2.0);
		}
		return e;
	}
}