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
 *   Created on Jul 20, 2018 by bjoern
 */
package org.knime.bigdata.commons.testing;

import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.workflow.FlowVariable;

/**
 * Holds definitions of flow variable names that are used for big data testflows.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
public enum TestflowVariable {

        /**
         * Shared flow variable for multiple connection types (HDFS, Hive, Impala, SSH).
         */
        HOSTNAME("hostname"),

        /**
         * Flow variable for the "authenticationmethod" setting of the HDFS connection node. Possible values: Password
         * (use also when using credentials), Kerberos
         */
        HDFS_AUTH_METHOD("hdfs.authMethod"),

        /**
         * Flow variable for the "useworkflowcredentials" setting of the HDFS connection node. Possible values: true,
         * false.
         * <p>
         * If true, then a credentials flow variable called "hdfs.credentials" will be automatically injected. The
         * username of the injected credentials is taken from {@link #HDFS_USERNAME}.
         * </p>
         */
        HDFS_USECREDENTIALS("hdfs.useCredentials", Type.BOOLEAN),

        /**
         * Testflow variable (not sure why we have it).
         */
        HDFS_URL("hdfs.url"),

        /**
         * Flow variable for the "user" setting of the HDFS connection node. Only applicable when
         * {@link #HDFS_AUTH_METHOD} is "Password".
         *
         * @see #HDFS_USECREDENTIALS
         */
        HDFS_USERNAME("hdfs.username"),

        /**
         * Flow variable with the WebHDFS port.
         */
        HDFS_WEBHDFS_PORT("hdfs.webhdfs.port", Type.INT),

        /**
         * Flow variable for the "database" setting of the Hive connection node.
         */
        HIVE_DATABASENAME("hive.databasename"),

        /**
         * Flow variable for the "parameter" setting of the Hive connection node.
         */
        HIVE_PARAMETER("hive.parameter", Type.STRING, true),

        /**
         * Flow variable for the "port" setting of the Hive Connection.
         */
        HIVE_PORT("hive.port", Type.INT),

        /**
         * Flow variable for the "kerberos" setting of the Hive connection node. Possible values: true, false.
         *
         * <p>
         * If false, then a credentials flow variable called "hive.credentials" will be automatically injected. The
         * username and password of the injected credentials are taken from {@link #HIVE_USERNAME} and
         * {@link #HIVE_PASSWORD}.
         * </p>
         */
        HIVE_USE_KERBEROS("hive.useKerberos", Type.BOOLEAN),

        /**
         * Username for the credentials flow variable called "hive.credentials" that will be automatically injected,
         * when {@link #HIVE_USE_KERBEROS} is false.
         */
        HIVE_USERNAME("hive.username"),

        /**
         * Password for the credentials flow variable called "hive.credentials" that will be automatically injected,
         * when {@link #HIVE_USE_KERBEROS} is false.
         */
        HIVE_PASSWORD("hive.password", Type.STRING, true),

        /**
         * Flow variable for the "database" setting of the Impala Connection node.
         */
        IMPALA_DATABASENAME("impala.databasename"),

        /**
         * Flow variable for the "parameter" setting of the Impala Connection node.
         */
        IMPALA_PARAMETER("impala.parameter", Type.STRING, true),

        /**
         * Flow variable for the "kerberos" setting of the Impala Connection node. Possible values: true, false.
         *
         * <p>
         * If false, then a credentials flow variable called "impala.credentials" will be automatically injected. The
         * username and password of the injected credentials are taken from {@link #IMPALA_USERNAME} and
         * {@link #IMPALA_PASSWORD}.
         * </p>
         */
        IMPALA_USE_KERBEROS("impala.useKerberos", Type.BOOLEAN),

        /**
         * Username for the credentials flow variable called "impala.credentials" that will be automatically injected,
         * when {@link #IMPALA_USE_KERBEROS} is false.
         */
        IMPALA_USERNAME("impala.username"),

        /**
         * Password for the credentials flow variable called "impala.credentials" that will be automatically injected,
         * when {@link #IMPALA_USE_KERBEROS} is false.
         */
        IMPALA_PASSWORD("impala.password", Type.STRING, true),

        /**
         * Flow variable for the "port" setting of the SSH Connection node.
         */
        SSH_PORT("ssh.port", Type.INT),

        /**
         * Username for the credentials flow variable called "ssh.credentials" that will be automatically injected.
         */
        SSH_USERNAME("ssh.username"),

        /**
         * Password for the credentials flow variable called "ssh.credentials" that will be automatically injected.
         * The password will be used to open the keyfile.
         */
        SSH_PASSWORD("ssh.password", Type.STRING, true),

        /**
         * Flow variable for the "keyfile" setting of the SSH Connection node.
         */
        SSH_KEYFILE("ssh.keyfile"),

        /**
         * Flow variable that will be picked up by the "Create Big Data Test Environment" node to decide which kind of
         * Spark context to create. See {@link org.knime.bigdata.spark.core.context.SparkContextIDScheme} for possible
         * values.
         */
        @SuppressWarnings("javadoc")
        SPARK_CONTEXTIDSCHEME("spark.contextIdScheme"),

        /**
         * Flow variable that will be picked up by the "Create Big Data Test Environment" node to decide which version
         * of Spark to configure on the created Spark context (if applicable to context type).
         */
        SPARK_VERSION("spark.version"),

        /**
         * Flow variable that will be picked up by the "Create Big Data Test Environment" node to decide whether to
         * configure custom Spark settings on the created Spark context (if applicable to context type).
         */
        SPARK_SETTINGSOVERRIDE("spark.settingsOverride", Type.BOOLEAN),

        /**
         * Flow variable that will be picked up by the "Create Big Data Test Environment" node to configure custom Spark
         * settings on the created Spark context (if applicable to context type).
         */
        SPARK_SETTINGSCUSTOM("spark.settingsCustom", Type.STRING, true),

        /**
         * Flow variable for the "v1_6.jobServerUrl" node setting of the "Create Spark Context" node. Also, this flow
         * variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when creating
         * a Spark Jobserver Spark context.
         */
        SPARK_SJS_URL("spark.sjs.url"),

        /**
         * Flow variable for the "v1_6.contextName" node setting of the "Create Spark Context" node. Also, this flow
         * variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when creating
         * a Spark Jobserver Spark context.
         */
        SPARK_SJS_CONTEXTNAME("spark.sjs.contextName"),

        /**
         * Flow variable for the "v1_6.authentication -> _selectedType" node setting of the "Create Spark Context" node.
         * Also, this flow variable will be picked up for the same purpose by the "Create Big Data Test Environment"
         * node when creating a Spark Jobserver Spark context. Possible values are: NONE, USER_PWD, CREDENTIALS
         *
         * <p>
         * A credentials flow variable called "spark.sjs.credentials" will be automatically injected. The username and
         * password of the injected credentials are taken from {@link #SPARK_SJS_USERNAME} and
         * {@link #SPARK_SJS_PASSWORD}.
         * </p>
         */
        SPARK_SJS_AUTHMETHOD("spark.sjs.authMethod"),

        /**
         * Flow variable for the "v1_6.authentication -> _username" node setting of the "Create Spark Context" node.
         * Also, this flow variable will be picked up for the same purpose by the "Create Big Data Test Environment"
         * node when creating a Spark Jobserver Spark context.
         */
        SPARK_SJS_USERNAME("spark.sjs.username"),

        /**
         * Flow variable for the "v1_6.authentication -> _password" node setting of the "Create Spark Context" node.
         * Also, this flow variable will be picked up for the same purpose by the "Create Big Data Test Environment"
         * node when creating a Spark Jobserver Spark context.
         */
        SPARK_SJS_PASSWORD("spark.sjs.password", Type.STRING, true),

        /**
         * Flow variable for the "v1_6.sparkReceiveTimeout" node setting of the "Create Spark Context" node. Also, this
         * flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a Spark Jobserver Spark context.
         */
        SPARK_SJS_RECEIVETIMEOUT("spark.sjs.receiveTimeout", Type.INT),

        /**
         * Flow variable for the "contextName" node setting of the "Create Local Big Data Environment" node. Also, this
         * flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a local Spark context.
         */
        SPARK_LOCAL_CONTEXTNAME("spark.local.contextName"),

        /**
         * Flow variable for the "numberOfThreads" node setting of the "Create Local Big Data Environment" node. Also,
         * this flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a local Spark context.
         */
        SPARK_LOCAL_THREADS("spark.local.threads", Type.INT),

        /**
         * Flow variable for the "sqlSupport" node setting of the "Create Local Big Data Environment" node. Also, this
         * flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a local Spark context. Possible values are: HIVEQL_WITH_JDBC, HIVEQL_ONLY, SPARKSQL_ONLY
         *
         * @see org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.SQLSupport
         */
        @SuppressWarnings("javadoc")
        SPARK_LOCAL_SQLSUPPORT("spark.local.sqlSupport"),

        /**
         * This flow variable will be picked up by the "Create Big Data Test Environment" node when creating a local
         * Spark context when sqlSupport is HIVEQL_WITH_JDBC. It defines the TCP port that the Spark Thriftserver will
         * listen on.
         */
        SPARK_LOCAL_THRIFTSERVERPORT("spark.local.thriftserverPort", Type.INT),

        /**
         * Flow variable for the "useHiveDataFolder" node setting of the "Create Local Big Data Environment" node. Also,
         * this flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a local Spark context.
         */
        SPARK_LOCAL_USEHIVEDATAFOLDER("spark.local.useHiveDataFolder", Type.BOOLEAN),

        /**
         * Flow variable for the "hiveDataFolder" node setting of the "Create Local Big Data Environment" node. Also,
         * this flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a local Spark context.
         */
        SPARK_LOCAL_HIVEDATAFOLDER("spark.local.hiveDataFolder", Type.STRING, true),

        /**
         * Flow variable for the "livyUrl" node setting of the "Create Spark Context via Livy" node. Also, this flow
         * variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when creating
         * a Spark context in Livy.
         */
        SPARK_LIVY_URL("spark.livy.url"),

        /**
         * Flow variable for the "authentication -> selectedType" node setting of the "Create Spark Context via Livy"
         * node. Also, this flow variable will be picked up for the same purpose by the "Create Big Data Test
         * Environment" node when creating a Spark context in Livy. Possible values are: NONE, KERBEROS
         */
        SPARK_LIVY_AUTHMETHOD("spark.livy.authMethod"),

        /**
         * Flow variable for the "setStagingAreaFolder" node setting of the "Create Spark Context via Livy" node. Also,
         * this flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a Spark context in Livy. Possible values are: true, false
         */
        SPARK_LIVY_SETSTAGINGAREAFOLDER("spark.livy.setStagingAreaFolder", Type.BOOLEAN),

        /**
         * Flow variable for the "stagingAreaFolder" node setting of the "Create Spark Context via Livy" node. Also,
         * this flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a Spark context in Livy.
         */
        SPARK_LIVY_STAGINGAREAFOLDER("spark.livy.stagingAreaFolder"),

        /**
         * Flow variable for the "connectTimeout" node setting of the "Create Spark Context via Livy" node. Also, this
         * flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a Spark context in Livy.
         */
        SPARK_LIVY_CONNECTTIMEOUT("spark.livy.connectTimeout", Type.INT),

        /**
         * Flow variable for the "requestTimeout" node setting of the "Create Spark Context via Livy" node. Also, this
         * flow variable will be picked up for the same purpose by the "Create Big Data Test Environment" node when
         * creating a Spark context in Livy.
         */
        SPARK_LIVY_RESPONSETIMEOUT("spark.livy.responseTimeout", Type.INT);

    /**
     * Type enum for flow variables.
     *
     * @author Bjoern Lohrmann, KNIME GmbH
     */
    public enum Type {
            /**
             * For flow variables that can be parsed into an int.
             */
            INT,

            /**
             * For string flow variables.
             */
            STRING,

            /**
             * For flow variables that can be parsed into a boolean.
             */
            BOOLEAN;
    }

    private final static Map<String, TestflowVariable> TEXT2ENUM = new HashMap<>();
    static {
        for (TestflowVariable flowVar : TestflowVariable.values()) {
            TEXT2ENUM.put(flowVar.m_name, flowVar);
        }
    }

    private final String m_name;

    private final Type m_type;

    private final boolean m_blankValuesAllowed;

    private TestflowVariable(final String name) {
        this(name, Type.STRING, false);
    }

    private TestflowVariable(final String name, final Type type) {
        this(name, type, false);
    }

    private TestflowVariable(final String name, final Type type, final boolean blankValuesAllowed) {
        m_name = name;
        m_type = type;
        m_blankValuesAllowed = blankValuesAllowed;
    }

    /**
     *
     * @return true when, the flow variable stores strings.
     */
    public boolean isString() {
        return m_type == Type.STRING;
    }

    /**
     *
     * @return true, when the flow variable can be parsed into an int.
     */
    public boolean isInt() {
        return m_type == Type.INT;
    }

    /**
     *
     * @return true, when the flow variable can be parsed into a boolean.
     */
    public boolean isBoolean() {
        return m_type == Type.BOOLEAN;
    }

    /**
     * Maps the given string to an enum value of {@link TestflowVariable}.
     *
     * @param name Name of the flow variable.
     * @return an enum value of {@link TestflowVariable}
     * @throws IllegalArgumentException when the given string does not map to an enum value of {@link TestflowVariable}.
     */
    public static TestflowVariable fromName(final String name) {
        final TestflowVariable flowVar = TEXT2ENUM.get(name);
        if (flowVar == null) {
            throw new IllegalArgumentException("Unknown testflow variable name: " + name);
        } else {
            return flowVar;
        }
    }

    /**
     *
     * @return the value type of this flow variable.
     */
    public Type getType() {
        return m_type;
    }

    /**
     * Validates the given value for this flow variable.
     *
     * @param value A candidate value for this variable.
     * @throws IllegalArgumentException when the given candidate value is not applicable for the flow variable.
     */
    public void validateValue(final String value) {
        // this check is here and not in the constructor because otherwise initializiation of the enum fields may fail
        // which may result in strange NoClassDefFoundErrors
        if (m_type != Type.STRING && m_blankValuesAllowed) {
            throw new IllegalArgumentException("Blank values are only possible for flow variables of type String");
        }

        switch (m_type) {
            case INT:
                Integer.parseInt(value);
                break;
            case BOOLEAN:
                if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException(
                        String.format("Value '%s' for flow variable %s is not a parseable boolean.", value, m_name));
                }
                break;
            default: // STRING
                if (!m_blankValuesAllowed && value.isEmpty()) {
                    throw new IllegalArgumentException(
                        String.format("Flow variable %s must not have a blank value.", m_name));
                }
                break;
        }
    }

    /**
     *
     * @return the name of the flow variable.
     */
    public String getName() {
        return m_name;
    }

    /**
     * Retrieves the value of the given flow variable from the map and tests whether it is "true".
     *
     * @param flowVar The flow variable whose value should be parsed.
     * @param flowVars Maps flow variables to their values.
     * @return true, when the value of the given flow variable is "true", false otherwise.
     */
    public static boolean isTrue(final TestflowVariable flowVar, final Map<String, FlowVariable> flowVars) {
        return flowVars.containsKey(flowVar.getName())
            && flowVars.get(flowVar.getName()).getStringValue().equalsIgnoreCase("true");
    }

    /**
     * Retrieves the value of the given flow variable from the map and tests whether it equals the given string.
     *
     * @param flowVar The flow variable whose value should be tested.
     * @param expectedValue The string to compare against for equality.
     * @param flowVars Maps flow variables to their values.
     * @return true, when the value of the given flow variable equals the expected value.
     */
    public static boolean stringEquals(final TestflowVariable flowVar, final String expectedValue,
        final Map<String, FlowVariable> flowVars) {
        return flowVars.containsKey(flowVar.getName())
            && flowVars.get(flowVar.getName()).getStringValue().equals(expectedValue);
    }

    /**
     * Retrieves the value of the given flow variable from the map and tests whether it equals the given string
     * (ignoring case).
     *
     * @param flowVar The flow variable whose value should be tested.
     * @param expectedValue The string to compare against for equality.
     * @param flowVars Maps flow variables to their values.
     * @return true, when the value of the given flow variable equals the expected value (ignoring case).
     */
    public static boolean stringEqualsIgnoreCase(final TestflowVariable flowVar, final String expectedValue,
        final Map<String, FlowVariable> flowVars) {

        return flowVars.containsKey(flowVar.getName())
            && flowVars.get(flowVar.getName()).getStringValue().equals(expectedValue);
    }

    /**
     * Retrieves the value of the given flow variable from the map.
     *
     * @param flowVar The flow variable whose value should be obtained.
     * @param flowVars Maps flow variables to their values.
     * @return the value assigned to the flow variable.
     */
    public static String getString(final TestflowVariable flowVar, final Map<String, FlowVariable> flowVars) {
        return flowVars.get(flowVar.getName()).getStringValue();
    }

    /**
     * Retrieves the value of the given flow variable from the map and parses it as an int.
     *
     * @param flowVar The flow variable whose value should be obtained.
     * @param flowVars Maps flow variables to their values.
     * @return the int value pasred from the value of the flow variable.
     */
    public static int getInt(final TestflowVariable flowVar, final Map<String, FlowVariable> flowVars) {
        return flowVars.get(flowVar.getName()).getIntValue();
    }
}
