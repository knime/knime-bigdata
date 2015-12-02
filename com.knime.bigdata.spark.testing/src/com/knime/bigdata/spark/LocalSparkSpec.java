package com.knime.bigdata.spark;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;
import org.junit.Rule;
import org.junit.rules.ExternalResource;

import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;


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
    
    /**
     * @return
     */
    protected JavaRDD<Row> serialize2RDD(final Object[][] aData) {
    	List<Row> rows = new ArrayList<>();
    	for (Object[] row : aData) {
    		RowBuilder rb = RowBuilder.emptyRow();
    		for (Object o : row) {
    			rb.add(o);
    		}
    		rows.add(rb.build());
    	}
    	return sparkContextResource.sparkContext.parallelize(rows);
    }
}
