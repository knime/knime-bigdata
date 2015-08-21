/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 26.05.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.spark.sql.api.java.StructType;

import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

/**
 * immutable container for job results
 *
 * a job result may contain a message, any number of table keys and corresponding schemas and at most one mllib model or
 * some other result object (some object in fact)
 *
 * @author dwk
 */
public class JobResult implements Serializable {

    /**
     *
     */
    private static final String MSG_IDENTIFIER = "msg";

    /**
     *
     */
    private static final String TABLES_IDENTIFIER = "tables";

    /**
     *
     */
    private static final String OBJECT_IDENTIFIER = "model";

    private static final String STATUS_IDENTIFIER = "status";

    private static final String WARNING_IDENTIFIER = "warn";

    private static final String ERROR_IDENTIFIER = "error";

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * some String that is (part of) the result
     */
    private final String m_msg;

    /**
     * schemas of tables that were created by this job (if any)
     */
    private final Map<String, StructType> m_tables;

    /**
     * machine learning model learned by this job (if any), or some other job result
     */
    private final Serializable m_object;

    private final boolean m_isError;

    private final ArrayList<String[]> m_warn = new ArrayList<>();

    private final ArrayList<String[]> m_error = new ArrayList<>();

    private JobResult(final String aMsg, final Map<String, StructType> aTables, final Serializable aObjectResult) {
        this(aMsg, aTables, aObjectResult, null);
    }

    private JobResult(final String aMsg, final Map<String, StructType> aTables, final Serializable aObjectResult,
        final String aStacktrace) {
        m_msg = aMsg;
        m_tables = aTables;
        if (aStacktrace != null) {
            m_object = aStacktrace;
            m_isError = true;
        } else {
            m_object = aObjectResult;
            m_isError = false;
        }
    }

    /**
     * create dummy job result without any content
     *
     * @return empty JobResult
     */
    public static JobResult emptyJobResult() {
        return new JobResult("", Collections.<String, StructType> emptyMap(), null);
    }

    /**
     * set the message value of this job result
     *
     * @param aMsg the (new) message
     * @return copy of this with (new) message set
     */
    public JobResult withMessage(@Nonnull final String aMsg) {
        return new JobResult(aMsg, m_tables, m_object);
    }

    /**
     * add a table to the map of tables contained in this result
     *
     * @param aKey table identifier
     * @param aTableSchema table schema
     * @return copy of this with additional table schema
     */
    public JobResult withTable(@Nonnull final String aKey, @Nullable final StructType aTableSchema) {
        final Map<String, StructType> tables = new HashMap<>(m_tables);
        tables.put(aKey, aTableSchema);
        return new JobResult(m_msg, Collections.unmodifiableMap(tables), m_object);
    }

    /**
     * add a throwable to this result
     *
     * @param aThrowable the error
     * @return copy of this with error set
     */
    public JobResult withException(final Throwable aThrowable) {
        return new JobResult(m_msg, m_tables, m_object, aThrowable.getStackTrace().toString());
    }

    /**
     * set model value of this serializable object
     *
     * @param aObjectResult the (new) model or other result object
     * @return copy of this with (new) model / object set
     */
    public JobResult withObjectResult(final Serializable aObjectResult) {
        return new JobResult(m_msg, m_tables, aObjectResult);
    }

    /**
     * de-serialize a job result from a config / base-64 string
     *
     * @param aStringRepresentation
     * @return de-serialized job result from given base-64 string
     */
    @SuppressWarnings("unchecked")
    public static JobResult fromBase64String(@Nonnull final String aStringRepresentation) {
        Config config = ConfigFactory.parseString(aStringRepresentation);
        final String msg = config.getString(MSG_IDENTIFIER);
        final boolean isError = config.getBoolean(STATUS_IDENTIFIER);
        final Map<String, StructType> m = new HashMap<>();
        ConfigObject c = config.getObject(TABLES_IDENTIFIER);
        for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
            final ConfigValue value = entry.getValue();
            if (value != null && value.unwrapped() instanceof String) {
                final StructType structType = StructTypeBuilder.fromConfigString((String)value.unwrapped());
                m.put(stripQuotes(entry.getKey()), structType);
            } else {
                m.put(stripQuotes(entry.getKey()), null);
            }
        }
        final List<String[]> warn = new ArrayList<>();
        final List<String[]> error = new ArrayList<>();
        try {
            final JobConfig jobConfig = new JobConfig(config);
            if (isError) {
                return new JobResult(msg, Collections.unmodifiableMap(m), null,
                    (String)(jobConfig.decodeFromParameter(OBJECT_IDENTIFIER)));
            }
            if (config.hasPath(OBJECT_IDENTIFIER)) {
                return new JobResult(msg, Collections.unmodifiableMap(m),
                    (Serializable)(jobConfig.decodeFromParameter(OBJECT_IDENTIFIER)));
            }
            warn.addAll((List<String[]>)jobConfig.decodeFromParameter(WARNING_IDENTIFIER));
            error.addAll((List<String[]>)jobConfig.decodeFromParameter(ERROR_IDENTIFIER));

        } catch (GenericKnimeSparkException e) {
            return JobResult.emptyJobResult().withMessage(msg).withException(e);
        }
        JobResult res = new JobResult(msg, Collections.unmodifiableMap(m), null);
        res.addErrors(error);
        res.addWarnings(warn);
        return res;
    }

    /**
     * @return config / base-64 string representation of this object
     */
    @Override
    public String toString() {

        Config config =
            ConfigFactory.empty().withValue(MSG_IDENTIFIER, ConfigValueFactory.fromAnyRef(ensureQuotes(m_msg)));

        final Map<String, String> m = new HashMap<>();
        for (Map.Entry<String, StructType> entry : m_tables.entrySet()) {
            final String key = ensureQuotes(entry.getKey());
            final StructType value = entry.getValue();
            if (value != null) {
                final String structConfig = StructTypeBuilder.toConfigString(value);
                m.put(key, structConfig);
            } else {
                m.put(key, null);
            }
        }
        config = config.withValue(TABLES_IDENTIFIER, ConfigValueFactory.fromMap(m));
        config = config.withValue(STATUS_IDENTIFIER, ConfigValueFactory.fromAnyRef(m_isError));
        if (m_object != null) {
            try {
                config =
                    config.withValue(OBJECT_IDENTIFIER,
                        ConfigValueFactory.fromAnyRef(JobConfig.encodeToBase64(m_object)));
            } catch (GenericKnimeSparkException e) {
                e.printStackTrace();
                System.err.println("Unable to convert result object to base 64 string. Object will be dropped.");
            }
        }
        try {
            config =
                config.withValue(WARNING_IDENTIFIER, ConfigValueFactory.fromAnyRef(JobConfig.encodeToBase64(m_warn)));
            config =
                config.withValue(ERROR_IDENTIFIER, ConfigValueFactory.fromAnyRef(JobConfig.encodeToBase64(m_error)));
        } catch (GenericKnimeSparkException e) {
            e.printStackTrace();
            System.err.println("Unable to convert error / warning messages to base 64 string. Messages will be dropped.");
        }
        return config.root().render(ConfigRenderOptions.concise());

    }

    /**
     * ensure that the given string starts and ends with quotes
     *
     * @param aString
     * @return original string if it is already in quotes, quoted string otherwise
     */
    private static String ensureQuotes(final String aString) {
        if (aString == null) {
            return "";
        }
        if (!aString.startsWith("\"") || !aString.endsWith("\"")) {
            return "\"" + aString + "\"";
        }
        return aString;
    }

    /**
     * remove quotes from start/end of given string
     *
     * @param aString
     * @return original string if it is not in quotes, un-quoted string otherwise
     */
    private static String stripQuotes(final String aString) {
        if (aString.startsWith("\"") && aString.endsWith("\"")) {
            return aString.substring(1, aString.length() - 1);
        }
        return aString;
    }

    /**
     * @return job result message, may be empty string, but never null
     */
    @Nonnull
    public String getMessage() {
        return m_msg;
    }

    /**
     * @return true if job terminated with an error
     */
    public boolean isError() {
        return m_isError;
    }

    /**
     * @return job result tables, may be empty set, but never null
     */
    @Nonnull
    public Set<String> getTableNames() {
        return m_tables.keySet();
    }

    /**
     * @param aTableName name of table to retrieve (key in collection of named rdds)
     * @return StructType of job result table with given name, might be null if computation failed or struct type was
     *         not computed (not all jobs compute the struct type)
     */
    @CheckForNull
    public StructType getTableStructType(final String aTableName) {
        return m_tables.get(aTableName);
    }

    /**
     * @return job result object, could be null
     */
    @CheckForNull
    public Object getObjectResult() {
        return m_object;
    }

    /**
     * @return key of some table, if at least one table is stored, null otherwise
     */
    @CheckForNull
    public String getFirstTableKey() {
        if (m_tables.size() > 0) {
            String key = m_tables.keySet().iterator().next();
            if (key.startsWith("\"") && key.endsWith("\"")) {
                return key.substring(1, key.length() - 1);
            }
            return key;
        }
        return null;
    }

    /**
     * @return list of server warnings, empty if there were none
     */
    public List<String[]> getWarnings() {
        return m_warn;
    }

    /**
     * @return list of server error messages, empty if there were none
     */
    public List<String[]> getErrors() {
        return m_error;
    }

    /**
     * @param aWarningMessages
     */
    public void addWarnings(final List<String[]> aWarningMessages) {
        m_warn.addAll(aWarningMessages);
    }

    /**
     * @param aErrorMessages
     */
    public void addErrors(final List<String[]> aErrorMessages) {
        m_error.addAll(aErrorMessages);
    }

}
