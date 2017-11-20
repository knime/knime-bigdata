package org.knime.bigdata.spark1_3.jobs.fetchrows;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.data.FetchRowsJobInput;
import org.knime.bigdata.spark.core.port.data.FetchRowsJobOutput;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateArrayDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark1_3.api.NamedObjects;
import org.knime.bigdata.spark1_3.api.SparkJob;
import org.knime.bigdata.spark1_3.api.TypeConverters;

/**
 * SparkJob that fetches and serializes a number of rows from the specified RDD (some other job must have previously
 * stored this RDD under this name in the named rdds map)
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class FetchRowsJob implements SparkJob<FetchRowsJobInput, FetchRowsJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(FetchRowsJob.class.getName());

    @Override
    public FetchRowsJobOutput runJob(final SparkContext sparkContext, final FetchRowsJobInput config,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final JavaRDD<Row> inputRDD = namedObjects.getJavaRdd(config.getFirstNamedInputObject());
        final int numRows = config.getNumberOfRows();

        LOGGER.log(Level.INFO, "Fetching " + numRows + " rows from input RDD");

        final List<Row> res;
        if (numRows > 0) {
            res = inputRDD.take(numRows);
        } else {
            res = inputRDD.collect();
        }

        return FetchRowsJobOutput.create(mapToListOfLists(res, config.getSpec(config.getFirstNamedInputObject())));
    }

    private List<List<Serializable>> mapToListOfLists(final List<Row> aRows, final IntermediateSpec spec) {

        final int numFields = spec.getNoOfFields();
        final IntermediateToSparkConverter<DataType>[] converters = TypeConverters.getConverters(spec);
        final IntermediateField fieldSpecs[] = spec.getFields();

        List<List<Serializable>> rows = new ArrayList<>(aRows.size());
        for (Row row : aRows) {
            final List<Serializable> convertedRow = new ArrayList<>(numFields);
            for (int j = 0; j < numFields; j++) {
                if (row.isNullAt(j)) {
                    convertedRow.add(null);
                } else if (fieldSpecs[j].getType() instanceof IntermediateArrayDataType) {
                    convertedRow.add(converters[j].convert((Object) row.getList(j).toArray()));
                } else {
                    convertedRow.add(converters[j].convert(row.get(j)));
                }
            }
            rows.add(convertedRow);
        }
        return rows;
    }
}