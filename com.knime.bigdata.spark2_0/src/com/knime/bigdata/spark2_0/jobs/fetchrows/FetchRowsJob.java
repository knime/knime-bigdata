package com.knime.bigdata.spark2_0.jobs.fetchrows;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.port.data.FetchRowsJobInput;
import com.knime.bigdata.spark.core.port.data.FetchRowsJobOutput;
import com.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateArrayDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SparkJob;
import com.knime.bigdata.spark2_0.api.TypeConverters;

/**
 * SparkJob that fetches and serializes a number of rows from the specified data frame.
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class FetchRowsJob implements SparkJob<FetchRowsJobInput, FetchRowsJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(FetchRowsJob.class.getName());

    @Override
    public FetchRowsJobOutput runJob(final SparkContext sparkContext, final FetchRowsJobInput config,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final Dataset<Row> inputDataset = namedObjects.getDataFrame(config.getFirstNamedInputObject());
        final int numRows = config.getNumberOfRows();

        LOGGER.info("Fetching " + numRows + " rows from input data frame.");

        final List<Row> res;
        if (numRows > 0) {
            res = inputDataset.takeAsList(numRows);
        } else {
            res = inputDataset.collectAsList();
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