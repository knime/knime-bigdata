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
 *   Created on 22.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark2_1.jobs.scripting.python;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.PythonRunner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.scripting.python.PySparkOutRedirectException;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkJobInput;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkJobOutput;
import org.knime.bigdata.spark2_1.api.NamedObjects;
import org.knime.bigdata.spark2_1.api.SparkJob;
import org.knime.bigdata.spark2_1.api.TypeConverters;

import scala.collection.mutable.StringBuilder;

/**
 * Job for PySpark scripts
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
@SparkClass
public class PySparkJob implements SparkJob<PySparkJobInput, PySparkJobOutput> {

    private static final String ENCODING = "UTF-8";

    private static final long serialVersionUID = -3523554160144305457L;

    private static final Logger LOGGER = Logger.getLogger(PySparkJob.class.getName());

    private static final PySparkDataExchanger EXCHANGER = PySparkDataExchanger.SINGLETON_INSTANCE;

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    public PySparkJobOutput runJob(final SparkContext sparkContext, final PySparkJobInput input,
        final NamedObjects namedObjects) throws Exception {
        collectInputInformation(sparkContext, input, namedObjects);
        File pyFile = writePyFile(input);
        String[] resultFrames = getResultFrames(input);

        //Grab the System.out to get Error messages from the pySpark script
        PrintStream console = System.out;
        ByteArrayOutputStream outStream = redirectOutput();

        String[] arg = {pyFile.getAbsolutePath(), "", "pythonlaunch"};
        try {
            PythonRunner.main(arg);
        } catch (Exception ex) {
            LOGGER.info(ex);
            //outStream will be closed in finally
            throw new KNIMESparkException(outStream.toString(ENCODING));
        } finally {
            //Everything went fine reset the output
            resetOutput(console, outStream);
        }
        if (input.getNumRows() != -1) {
            //We are running a validation script. Create a preview of the output for the user
            throw new PySparkOutRedirectException(createOutputforConsole(input, resultFrames));
        }
        IntermediateSpec[] outSpecs = createOutputSpecs(input, namedObjects, resultFrames);

        return new PySparkJobOutput(resultFrames, outSpecs);
    }

    /**
     * Creates a String with the first 10 lines of the result dataframes
     */
    private static String createOutputforConsole(final PySparkJobInput input, final String[] resultFrames) {
        StringBuilder sb = new StringBuilder();
        sb.append("Execution finished.\n\n");
        for (int i = 0; i < resultFrames.length; i++) {

            String resultFrame = input.getUID() + "_resultDataFrame" + (i + 1);
            Dataset<Row> pydf = EXCHANGER.getDataFrame(resultFrame);
            sb.append("resultDataFrame" + (i + 1) + "(10 of " + pydf.count() + " rows):\n");
            sb.append(Arrays.toString(pydf.schema().fieldNames()).replaceAll(",", "|") + "\n");
            List<Row> rowList = pydf.takeAsList(10);
            for (Row row : rowList) {
                sb.append(row.mkString("[", "|", "]") + "\n");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Collects the input data frames and the spark context and session in the exchanger to make it available in Python
     *
     * @param sparkContext the current spark context
     * @param input the job input
     * @param namedObjects the named objects
     */
    @SuppressWarnings("resource")
    private static void collectInputInformation(final SparkContext sparkContext, final PySparkJobInput input,
        final NamedObjects namedObjects) {
        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

        for (int i = 0; i < input.getNamedInputObjects().size(); i++) {
            Dataset<Row> dataFrame = getDataFrame(namedObjects, input.getNamedInputObjects(), i);
            if (dataFrame != null) {
                int numRows = input.getNumRows();
                if (numRows != -1) {
                    //we are executing on just a sample
                    List<Row> list = dataFrame.takeAsList(numRows);
                    dataFrame = spark.createDataFrame(list, dataFrame.schema());
                }
                EXCHANGER.addDataFrame(input.getUID() + "_dataFrame" + (i + 1), dataFrame);
            }
        }

        EXCHANGER.setSession(spark);
        EXCHANGER.setContext(new JavaSparkContext(sparkContext));
    }

    private static IntermediateSpec[] createOutputSpecs(final PySparkJobInput input, final NamedObjects namedObjects,
        final String[] resultFrames) {
        IntermediateSpec[] outSpecs = new IntermediateSpec[resultFrames.length];
        for (int i = 0; i < resultFrames.length; i++) {
            String resultFrame = input.getUID() + "_resultDataFrame" + (i + 1);
            Dataset<Row> pydf = EXCHANGER.getDataFrame(resultFrame);
            outSpecs[i] = TypeConverters.convertSpec(pydf.schema());
            namedObjects.addDataFrame(resultFrames[i], pydf);
            EXCHANGER.deleteNamedDataFrame(resultFrame);
        }
        return outSpecs;
    }

    private static void resetOutput(final PrintStream console, final ByteArrayOutputStream outStream)
        throws UnsupportedEncodingException {
        System.setOut(console);
        System.out.print(outStream.toString(ENCODING));
        try {
            outStream.close();
        } catch (IOException e) {
            LOGGER.info("Could not close stream.", e);
        }
    }

    private static ByteArrayOutputStream redirectOutput() throws UnsupportedEncodingException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream o = new PrintStream(outStream, true, ENCODING);
        System.setOut(o);
        return outStream;
    }

    private static String[] getResultFrames(final PySparkJobInput input) {
        int numOutObjects = input.getNamedOutputObjects().size();
        String[] resultFrames = new String[numOutObjects];
        for (int i = 0; i < numOutObjects; i++) {
            resultFrames[i] = input.getNamedOutputObjects().get(i);
        }
        return resultFrames;
    }

    private static File writePyFile(final PySparkJobInput input) throws KNIMESparkException {

        File pyFile;
        try {
            pyFile = File.createTempFile("pyhtonScript_" + UUID.randomUUID().toString().replace('-', '_'), ".py");
            writeCodetoFile(input, pyFile);
        } catch (IOException e1) {
            throw new KNIMESparkException("Could not create tempfile for python code", e1);
        }

        return pyFile;
    }

    private static void writeCodetoFile(final PySparkJobInput input, final File pyFile) throws KNIMESparkException {
        try (BufferedWriter writer = Files.newBufferedWriter(pyFile.toPath(), Charset.forName(ENCODING))) {
            writer.write(input.getPyScript(), 0, input.getPyScript().length());
        } catch (IOException e) {
            throw new KNIMESparkException("Could not write to tempfile", e);
        }
    }

    private static Dataset<Row> getDataFrame(final NamedObjects namedObjects, final List<String> namedObjectsList,
        final int i) {
        if (namedObjectsList.size() > i) {
            return namedObjects.getDataFrame(namedObjectsList.get(i));
        } else {
            return null;
        }
    }
}
