package com.knime.bigdata.spark.jobserver.server;

import static org.junit.Assert.assertEquals;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class JobConfigTest {

    @Test
	public void conversionToBase64StringAndBackShouldNotChangeString() throws Exception {
	    final String s1 = "some string with *#üהצ %$ chars and spaces and new lines\nYEAH!";
	    Config config = ConfigFactory.empty().withValue("param", ConfigValueFactory.fromAnyRef(JobConfig.encodeToBase64(s1)));
	    JobConfig configWrapper = new JobConfig(config);
	    final String s2 = configWrapper.decodeFromParameter("param");
	    assertEquals("conversion should not change string", s1, s2);
	}

   @Test
   public void conversionToBase64StringAndBackShouldNotChangeKMeansModel() throws GenericKnimeSparkException {
       Vector[] clusterCenters = new Vector[2];
       clusterCenters[0] = Vectors.dense(new double[] {0.1, 0.2, 0.4});
       clusterCenters[1] = Vectors.dense(new double[] {1.1, 1.2, -0.4});
       final KMeansModel m = new KMeansModel(clusterCenters);
       Config config = ConfigFactory.empty().withValue("param", ConfigValueFactory.fromAnyRef(JobConfig.encodeToBase64(m)));
       JobConfig configWrapper = new JobConfig(config);

       final KMeansModel converted = configWrapper.decodeFromParameter("param");
       assertEquals("conversion should not change number of clusters", 2, converted.clusterCenters().length);
       for (int i=0; i<  converted.clusterCenters().length; i++) {
           assertEquals("conversion should not change cluster", m.clusterCenters()[i], converted.clusterCenters()[i]);
       }
   }

}
