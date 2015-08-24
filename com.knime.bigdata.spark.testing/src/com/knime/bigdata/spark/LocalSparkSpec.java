package com.knime.bigdata.spark;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Rule;
import org.junit.rules.ExternalResource;


public class LocalSparkSpec {
    protected static class SparkContextResource extends ExternalResource {
        private static final SparkConf conf = new SparkConf().setAppName(LocalSparkSpec.class.getSimpleName())
            .setMaster("local");

        public JavaSparkContext sparkContext;

        @Override
        protected void before() {
            sparkContext = new JavaSparkContext(conf);
        }

        @Override
        protected void after() {
            sparkContext.close();
        }
    }

    @Rule
    public final SparkContextResource sparkContextResource = new SparkContextResource();

    protected final Map<String, JavaDoubleRDD> m_randomRDDs = new HashMap<>();

    /**
     * @return
     */
    protected JavaDoubleRDD getRandomDoubleRDD(final long aNumRows, final int aNumCols) {
        JavaDoubleRDD cached = m_randomRDDs.get(aNumRows + "-" + aNumCols);
        if (cached == null) {
            cached = normalJavaRDD(sparkContextResource.sparkContext, aNumRows, aNumCols);
            m_randomRDDs.put(aNumRows + "-" + aNumCols, cached);
        }
        return cached;
    }
}
