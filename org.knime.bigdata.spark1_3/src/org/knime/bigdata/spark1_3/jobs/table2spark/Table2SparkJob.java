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
 *   Created on 24.07.2015 by dwk
 */
package org.knime.bigdata.spark1_3.jobs.table2spark;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.io.table.reader.Table2SparkJobInput;
import org.knime.bigdata.spark1_3.api.NamedObjects;
import org.knime.bigdata.spark1_3.api.RowBuilder;
import org.knime.bigdata.spark1_3.api.SparkJobWithFiles;
import org.knime.bigdata.spark1_3.api.TypeConverters;

/**
 *
 * @author dwk, Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class Table2SparkJob implements SparkJobWithFiles<Table2SparkJobInput, EmptyJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Table2SparkJob.class.getName());

    private List<Row> readFileIntoRows(final File inputFile, final IntermediateSpec sparkDataSpec) throws Exception {

        IntermediateToSparkConverter<DataType>[] converters = TypeConverters.getConverters(sparkDataSpec);
        final List<Row> rows = new ArrayList<>();
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(inputFile)))) {
            final long rowCount = in.readLong();
            final int columnCount = in.readInt();

            for (int i = 0; i < rowCount; i++) {
                RowBuilder row = RowBuilder.emptyRow();
                for (int j = 0; j < columnCount; j++) {
                    Serializable intermediateValue = (Serializable) in.readObject();
                    row.add(converters[j].convert(intermediateValue));
                }
                rows.add(row.build());
            }
        }

        return rows;
    }

    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final Table2SparkJobInput input,
        final List<File> inputFiles, final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        LOGGER.log(Level.INFO, "Inserting KNIME data table into RDD...");
        final List<Row> rowData = readFileIntoRows(inputFiles.get(0), input.getSpec(input.getFirstNamedOutputObject()));

        final JavaRDD<Row> rdd = new JavaSparkContext(sparkContext).parallelize(rowData);

        final String outputRdd = input.getFirstNamedOutputObject();
        LOGGER.log(Level.INFO, "Storing data rdd under key: " + outputRdd);
        namedObjects.addJavaRdd(outputRdd, rdd);

        return EmptyJobOutput.getInstance();
    }
}
