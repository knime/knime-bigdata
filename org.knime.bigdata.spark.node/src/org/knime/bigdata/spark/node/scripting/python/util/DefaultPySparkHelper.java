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
 *   Created on 28.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.util;

import java.util.Arrays;
import java.util.Set;

import javax.swing.text.BadLocationException;

import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.guarded.GuardedSection;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;

/**
 * Default PySpark helper implementation
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public abstract class DefaultPySparkHelper implements PySparkHelper{

    private final CompatibilityChecker m_checker;
    private final String m_exchangerPackage;

    /**
     * Constructs a Default PySpark Helper
     * @param checker the {@link CompatibilityChecker}
     * @param exchangerPackage String identifying the exchanger package
     */
    public DefaultPySparkHelper(final CompatibilityChecker checker, final String exchangerPackage) {
        super();
        m_checker = checker;
        m_exchangerPackage = exchangerPackage;
    }


    /**
     * Creates the comments for the UDF section based on the number of input and output frames
     * @param inCount the number of input frames
     * @param outCount the number of output frames
     * @return the default UDF Section for this node type
     */
    public static String createDefaultUDFSection(final int inCount, final int outCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("\t# Custom pySpark code \n");
        sb.append("\t# SparkSession can be used with variable spark \n");
        if(inCount > 0) {
            sb.append("\t# The input dataFrame(s): " + Arrays.toString(getInputFrames(inCount)) + "\n");
        }

        sb.append("\t# The output dataFrame(s) must be: " + Arrays.toString(getOutputFrames(outCount)) + "\n");

        if(inCount > 0) {
            for(String dataframe : getOutputFrames(outCount)) {
                sb.append("\t"+dataframe + " = "+ getInputFrames(inCount)[0] + "\n" );
            }
        }else {
            for(String dataframe : getOutputFrames(outCount)) {
                sb.append("\t" + dataframe +" = spark.emptyDataFrame()\n" );
            }
        }
        sb.append("\t\n\t\n\t\n\t\n");
        sb.append("\t# End of user code");
        return sb.toString();
    }


    /**
     * Creates an String array of the names input frames of the pySpark script
     * @param inCount the number of inputFrames to create
     * @return the string array of the input data frame names
     */
    public static String[] getInputFrames(final int inCount) {

        String[] inFrames = new String[inCount];
        for(int i = 0; i< inCount; i++) {
            inFrames[i] = "dataFrame" + (i+1);
        }
       return inFrames;
    }

    /**
     * Creates an String array of the names output frames of the pySpark script
     * @param outCount the number of output frames to create
     * @return the string array of the output data frame names
     */
    public static String[] getOutputFrames(final int outCount) {

        String[] outFrames = new String[outCount];
        for(int i = 0; i< outCount; i++) {
            outFrames[i] = "resultDataFrame" + (i+1);
        }
       return outFrames;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompatibilityChecker getChecker() {
        return m_checker;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
        return m_checker.supportSpark(sparkVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return m_checker.getSupportedSparkVersions();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void checkUDF(final PySparkDocument doc, final int outCount)
        throws InvalidSettingsException {
        try {
            String udf = doc.getTextBetween(PySparkDocument.GUARDED_BODY_START, PySparkDocument.GUARDED_BODY_END);
            udf = udf.replaceAll("(?m)^[\t]#.+", "");
            String[] outFrames = getOutputFrames(outCount);
            for (String frame : outFrames) {
                if (!udf.contains(frame)) {
                    throw new InvalidSettingsException(
                        String.format("User code does not contain necessary dataframe %s", frame));
                }
            }
        } catch (BadLocationException e) {
            //should never happen
            throw new IllegalStateException(e);
        }

    }


    /**
     * {@inheritDoc}
     */
    @Override
    public GuardedDocument createGuardedPySparkDocument(final int inCount, final int outCount) {
        PySparkDocument doc = new PySparkDocument();
        createAllSections(doc, inCount, outCount);

        return doc;
    }

    /**
     * Initially creates all Sections
     *
     * @param doc the document in which to create the sections
     * @throws BadLocationException
     */
    private void createAllSections(final PySparkDocument doc, final int inCount, final int outCount) {
        try {
            GuardedSection guardedImports = doc.getGuardedSection(PySparkDocument.GUARDED_IMPORTS);
            guardedImports.setText(createImportSection());

            doc.replaceBetween(PySparkDocument.GUARDED_IMPORTS, PySparkDocument.GUARDED_FLOW_VARIABLES,
                "#Custom imports \n ");

            GuardedSection guardedFLow = doc.getGuardedSection(PySparkDocument.GUARDED_FLOW_VARIABLES);
            guardedFLow.setText("#Flowvariables \n");

            doc.replaceBetween(PySparkDocument.GUARDED_FLOW_VARIABLES, PySparkDocument.GUARDED_BODY_START,
                "#Custom globals \n");

            GuardedSection guardedBodyStart = doc.getGuardedSection(PySparkDocument.GUARDED_BODY_START);
            guardedBodyStart.setText(createMainStart(getInputFrames(inCount), "xxx"));

            doc.replaceBetween(PySparkDocument.GUARDED_BODY_START, PySparkDocument.GUARDED_BODY_END,
                createDefaultUDFSection(inCount, outCount));

            GuardedSection guardedBodyEnd = doc.getGuardedSection(PySparkDocument.GUARDED_BODY_END);
            guardedBodyEnd.setText(createMainEnd(getOutputFrames(outCount), "xxx"));
        } catch (BadLocationException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateGuardedSectionsUIDs(final PySparkDocument doc, final int inCount, final int outCount,
        final String uid) {
        try {
            GuardedSection guardedBodyStart = doc.getGuardedSection(PySparkDocument.GUARDED_BODY_START);
            guardedBodyStart.setText(createMainStart(getInputFrames(inCount), uid));

            GuardedSection guardedBodyEnd = doc.getGuardedSection(PySparkDocument.GUARDED_BODY_END);
            guardedBodyEnd.setText(createMainEnd(getOutputFrames(outCount), uid));

        } catch (BadLocationException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    /**
     * @return the string for the import section
     */
    private String createImportSection() {
        StringBuilder sb = new StringBuilder();
        sb.append("# System imports \n");
        sb.append("import sys\n" + "from pyspark.mllib.common import _py2java, _java2py\n"
            + "from pyspark import SparkContext, SparkFiles\n" + "from pyspark.sql import SQLContext\n"
            + "from pyspark.serializers import PickleSerializer\n" + "from pyspark.sql import SparkSession\n"
            + "from pyspark.sql.types import *\n" + "from pyspark.java_gateway import launch_gateway\n"
            + "from pyspark.profiler import BasicProfiler\n");
        addExchanger(sb);
        return sb.toString();
    }

    /**
     *
     * @param dataFrames the names of the input dataFrames
     * @param uid the uid for the dataframes
     * @return string for the main start
     */
    private static String createMainStart(final String[] dataFrames, final String uid) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Initialization of Spark environment\n");
        sb.append("if __name__ == \"__main__\":\n");
        sb.append("\t_exchanger = Exchanger()\n");
        sb.append("\tspark = _exchanger.getSparkSession()\n");
        sb.append("\t# Get data from jvm \n");
        for (String dataFrameName : dataFrames) {
            appendInputDataFrame(sb, dataFrameName, uid);
        }

        sb.append("\t#End of initialization \n");
        sb.append("\t\n");

        return sb.toString();
    }

    private static String createMainEnd(final String[] namedOutputs, final String uid) {
        StringBuilder sb = new StringBuilder();
        sb.append("\t# Send data to jvm \n");
        for (String dataFrameName : namedOutputs) {
            appendOutputDataObject(sb, dataFrameName, uid);
        }
        return sb.toString();
    }

    private void addExchanger(final StringBuilder sb) {
        sb.append("class Exchanger(object):\n");
        sb.append("\t_spark = None\n");
        sb.append("\t_jexchange = None\n");

         sb.append("\tdef __init__ (self):\n"
             + "\t\t_gateway = launch_gateway()\n"
             + "\t\t_jvm = _gateway.jvm\n"
             + "\t\tself._jexchange  = "
             + "_jvm."+ m_exchangerPackage +".SINGLETON_INSTANCE\n"
             + "\t\tjcontext = self._jexchange.getContext()\n"
             + "\t\tjsession = self._jexchange.getSession()\n"
             + "\t\tsparkCon = SparkContext(None, None, None, None, None, 0, "
                 + "PickleSerializer(), None, _gateway, jcontext, BasicProfiler)\n"
             + "\t\tself._spark = SparkSession(sparkCon,jsession)\n");

         sb.append("\tdef getDataFrame(self, name):\n"
             + "\t\tjavain = self._jexchange.getDataFrame(name)\n"
             + "\t\tdf_in = _java2py(self._spark, javain)\n"
             + "\t\treturn df_in\n");

         sb.append("\tdef addDataFrame(self, name, df):\n"
             + "\t\tjdf = _py2java(self._spark, df)\n"
             + "\t\tself._jexchange.addDataFrame(name,jdf)\n");

         sb.append("\tdef addObject(self, name, obj):\n"
             + "\t\tjobj = _py2java(self._spark, obj)\n"
             + "\t\tself._jexchange.add(name,jobj)\n");

         sb.append("\tdef getObject(self, name):\n"
             + "\t\tjavain = self._jexchange.get(name)\n"
             + "\t\tobj = _java2py(self._spark, javain)\n"
             + "\t\treturn obj\n");

         sb.append("\tdef getSparkSession(self):\n"
             + "\t\treturn self._spark\n");

    }

    private static void appendInputDataFrame(final StringBuilder sb, final String dataFrameName, final String uid) {
        sb.append("\t" + dataFrameName + " = _exchanger.getDataFrame(\"" + uid + "_" + dataFrameName + "\")\n");
    }

    private static void appendOutputDataObject(final StringBuilder sb, final String dataFrameName, final String uid) {
          sb.append("\t_exchanger.addDataFrame(\"" + uid + "_" + dataFrameName + "\"," + dataFrameName + ")\n");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateGuardedSection(final PySparkDocument doc, final int inCount, final int outCount) {
        try {
            GuardedSection guardedImports = doc.getGuardedSection(PySparkDocument.GUARDED_IMPORTS);
            guardedImports.setText(createImportSection());

            GuardedSection guardedBodyStart = doc.getGuardedSection(PySparkDocument.GUARDED_BODY_START);
            guardedBodyStart.setText(createMainStart(getInputFrames(inCount), "xxx"));

            GuardedSection guardedBodyEnd = doc.getGuardedSection(PySparkDocument.GUARDED_BODY_END);
            guardedBodyEnd.setText(createMainEnd(getOutputFrames(outCount), "xxx"));
        } catch (BadLocationException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }

    }
}
