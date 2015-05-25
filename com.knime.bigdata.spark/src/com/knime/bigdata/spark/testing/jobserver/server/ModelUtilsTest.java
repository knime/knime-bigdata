package com.knime.bigdata.spark.testing.jobserver.server;

import static org.junit.Assert.assertEquals;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.testing.UnitSpec;


/**
 *
 * @author dwk
 *
 */
public class ModelUtilsTest extends UnitSpec {

    /**
     *
     */
	@Test
	public void conversionToBase64StringAndBackShouldNotChangeString() {
	    final String s1 = "some string with *#üהצ %$ chars and spaces and new lines\nYEAH!";
	    assertEquals("conversion should not change string", s1, ModelUtils.fromString(ModelUtils.toString(s1)));
	}

    /**
    *
    */
   @Test
   public void conversionToBase64StringAndBackShouldNotChangeKMeansModel() {
       Vector[] clusterCenters = new Vector[2];
       clusterCenters[0] = Vectors.dense(new double[] {0.1, 0.2, 0.4});
       clusterCenters[1] = Vectors.dense(new double[] {1.1, 1.2, -0.4});
       final KMeansModel m = new KMeansModel(clusterCenters);
       final KMeansModel converted = ModelUtils.fromString(ModelUtils.toString(m));
       assertEquals("conversion should not change number of clusters", 2, converted.clusterCenters().length);
       for (int i=0; i<  converted.clusterCenters().length; i++) {
           assertEquals("conversion should not change cluster", m.clusterCenters()[i], converted.clusterCenters()[i]);
       }
   }

}
