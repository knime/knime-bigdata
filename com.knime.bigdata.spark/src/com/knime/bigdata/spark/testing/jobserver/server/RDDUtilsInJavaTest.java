package com.knime.bigdata.spark.testing.jobserver.server;


import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.knime.bigdata.spark.jobserver.server.LabeledDataInfo;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;


/**
 *
 * @author dwk
 *
 */
public class RDDUtilsInJavaTest {
  private static class SparkContextResource extends ExternalResource {
    private static final SparkConf conf = new SparkConf().setAppName(RDDUtilsInJavaTest.class.getSimpleName()).setMaster(
        "local");

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
  public SparkContextResource sparkContextResource = new SparkContextResource();

  private static class MyMapper implements Serializable {
    private static final long serialVersionUID = 1L;

    JavaRDD<Vector> apply(final JavaDoubleRDD o) {
      return o.map(new Function<Double, Vector>() {
        private static final long serialVersionUID = 1L;
        private int ix = 0;

        @Override
        public Vector call(final Double x) {
          ix = ix + 1;
          return Vectors.dense(x, 2.0 * x, ix, x + 45);
        }
      });
    }

    JavaRDD<Row> toRowRdd(final JavaDoubleRDD o) {
      return o.map(new Function<Double, Row>() {
        private static final long serialVersionUID = 1L;
        private int ix = 0;

        @Override
        public Row call(final Double x) {
          ix = ix + 1;
          return Row.create(x, 2.0 * x, ix, x + 14);
        }
      });
    }

    final static String[] colors = { "red", "blue", "green", "black", "white" };
    final static String[] teams = { "FC1", "FC 2", "FC 1987", "Mein Klub" };

    JavaRDD<Row> toRowRddWithNominalValues(final JavaDoubleRDD o) {
      return o.map(new Function<Double, Row>() {
        private static final long serialVersionUID = 1L;
        private int ix = 0;

        @Override
        public Row call(final Double x) {
          ix = ix + 1;
          final String color = colors[(int) (colors.length * Math.random())];
          return Row.create(x, 2.0 * x, ix, color);
        }
      });
    }

    JavaRDD<String> extractLabelColumn(final JavaRDD<Row> aJavaRDDWithNominalLabels) {
        return aJavaRDDWithNominalLabels.map(new Function<Row, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(final Row x) {
              return x.getString(3);
            }
          });
    }

  }

  @Test
  public void whatDoesRowCreateDo() {
    Row row = Row.create(new Object[] { "a", 1, 2.8d });
    assertEquals("should have three elements", 3, row.length());
  }

  @Test
  public void conversionOfJavaPairedRDD2JavaRDDWithRows() throws Exception {
    // JavaRDD input1 = sparkContext.makeRDD();
    JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 2);
    JavaRDD<Vector> v = new MyMapper().apply(o);
    JavaRDD<Row> rowRDD = RDDUtils.toJavaRDDOfRows(v.zip(o));

    assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
    assertEquals("conversion should create correct length of rows ", 5, rowRDD.collect().get(0).length());
  }

  @Test
  public void conversionOfJavaRowRDD2JavaRDDWithVector() throws Exception {
    // JavaRDD input1 = sparkContext.makeRDD();
    JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 2);
    JavaRDD<Row> v = new MyMapper().toRowRdd(o);
    JavaRDD<Vector> rowRDD = RDDUtils.toJavaRDDOfVectors(v);

    assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
    assertEquals("conversion should create correct length of vectors ", 4, rowRDD.collect().get(0).size());
  }

  @Test
  public void addColumn2JavaRowRDD() throws Exception {
    // JavaRDD input1 = sparkContext.makeRDD();
    JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 2);
    JavaRDD<Row> v = new MyMapper().toRowRdd(o);

    JavaRDD<Row> rowRDD = RDDUtils.addColumn(v.zip(o));

    assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
    assertEquals("conversion should add single column ", 5, rowRDD.collect().get(0).length());
  }

  @Test
  public void conversionOfJavaRowRDD2JavaRDDWithVectorKeepOnlySomeFeatures() throws Exception {
    // JavaRDD input1 = sparkContext.makeRDD();
    JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 2);
    JavaRDD<Row> v = new MyMapper().toRowRdd(o);
    List<Integer> ix = new ArrayList<Integer>();
    ix.add(0);
    ix.add(1);
    ix.add(3);
    JavaRDD<Vector> rowRDD = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(v, ix);

    assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
    assertEquals("conversion should create correct length of vectors ", 3, rowRDD.collect().get(0).size());

  }

  @Test
  public void conversionOfJavaRowRDD2JavaRDDWithLabeledPoint() throws Exception {
    // JavaRDD input1 = sparkContext.makeRDD();
    JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 1);
    JavaRDD<Row> v = new MyMapper().toRowRdd(o);
    JavaRDD<LabeledPoint> rowRDD = RDDUtils.toJavaLabeledPointRDD(v, 2);

    assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);

    List<LabeledPoint> rows = rowRDD.collect();
    for (int i = 0; i < rows.size(); i++) {
      double[] features = rows.get(i).features().toArray();
      assertEquals("conversion should create correct length of vectors ", 3, features.length);
      for (int j = 0; j < features.length; j++) {
        assertTrue("label should not be contained in features", Math.abs(features[j] - i) > 0.0001);
      }
      assertEquals("conversion should set proper label ", 1 + i, (int) rows.get(i).label());
    }
  }

  @Test
  public void conversionOfJavaRowRDDWithNominalValues2JavaRDDWithLabeledPoint() throws Exception {
    JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 1);
    JavaRDD<Row> v = new MyMapper().toRowRddWithNominalValues(o).cache();
    StructType schema = StructTypeBuilder.fromRows(v.take(2)).build();
    LabeledDataInfo info = RDDUtilsInJava.toJavaLabeledPointRDDConvertNominalValues(v, schema, 3);

    assertEquals("Incorrect number of classes", 5, info.getNumberClasses());

    JavaRDD<LabeledPoint> rowRDD = info.getLabeledPointRDD();
    assertEquals("Conversion changed the number of rows ", rowRDD.count(), 100);

    List<LabeledPoint> rows = rowRDD.collect();
    List<String> labels = new MyMapper().extractLabelColumn(v).collect();
    for (int i = 0; i < rows.size(); i++) {
      double[] features = rows.get(i).features().toArray();
      assertEquals("conversion should create correct length of vectors ", 3, features.length);

      for (int j = 0; j < features.length; j++) {
        assertTrue("label should not be contained in features", Math.abs(features[j] - i) > 0.0001);
      }
      assertEquals("conversion should set proper label ",
    		  info.getClassLabelToIntMapping().get(labels.get(i)).intValue(), (int) rows.get(i).label());
    }
  }


  @Nonnull
  private static List<Row> repeatRowValues(final int numberOfRows, final Object... rowValues) {
    assert numberOfRows > 0;
    assert rowValues != null;

    final List<Row> rows = new ArrayList<>(numberOfRows);
    for (int i = 0; i < numberOfRows; ++i) {
      rows.add(Row.create(rowValues));
    }

    return rows;
  }

}