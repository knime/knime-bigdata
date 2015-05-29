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
 *   Created on 26.05.2015 by Dietrich
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
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

    private JobResult(final String aMsg, final Map<String, StructType> aTables, final Serializable aObjectResult) {
        m_msg = aMsg;
        m_tables = aTables;
        m_object = aObjectResult;
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
    public JobResult withTable(@Nonnull final String aKey, @Nonnull final StructType aTableSchema) {
        final Map<String, StructType> tables = new HashMap<>(m_tables);
        tables.put(aKey, aTableSchema);
        return new JobResult(m_msg, Collections.unmodifiableMap(tables), m_object);
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

    private Class<?> getJavaTypeFromDataType(final DataType aType) {
        for (Map.Entry<Class<?>, DataType> entry : StructTypeBuilder.DATA_TYPES_BY_CLASS.entrySet()) {
            if (entry.getValue().getClass().equals(aType.getClass())) {
                return entry.getKey();
            }
        }
        return Object.class;
    }

    /**
     * de-serialize a job result from a config / base-64 string
     *
     * @param aStringRepresentation
     * @return de-serialized job result from given base-64 string
     */
    public static JobResult fromBase64String(@Nonnull final String aStringRepresentation) {
        Config config = ConfigFactory.parseString(aStringRepresentation);
        final String msg = config.getString(MSG_IDENTIFIER);
        final Map<String, StructType> m = new HashMap<>();
        ConfigObject c = config.getObject(TABLES_IDENTIFIER);
        for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
            ConfigList types = (ConfigList)entry.getValue();
            StructField[] fields = new StructField[types.size()];
            int f = 0;
            for (ConfigValue v : types) {
                List<Object> dt = ((ConfigList)v).unwrapped();
                DataType t;
                try {
                    t = StructTypeBuilder.DATA_TYPES_BY_CLASS.get(Class.forName(dt.get(1).toString()));
                    fields[f++] =
                        DataType.createStructField(dt.get(0).toString(), t, Boolean.parseBoolean(dt.get(2).toString()));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            StructType structType = DataType.createStructType(fields);
            m.put(entry.getKey(), structType);
        }
        if (config.hasPath(OBJECT_IDENTIFIER)) {
            return new JobResult(msg, Collections.unmodifiableMap(m), (Serializable)ModelUtils.fromString(config
                .getString(OBJECT_IDENTIFIER)));
        }
        return new JobResult(msg, Collections.unmodifiableMap(m), null);
    }

    /**
     * @return config / base-64 string representation of this object
     */
    @Override
    public String toString() {

        Config config =
            ConfigFactory.empty().withValue(MSG_IDENTIFIER, ConfigValueFactory.fromAnyRef(ensureQuotes(m_msg)));

        Map<String, ArrayList<ArrayList<String>>> m = new HashMap<>();
        for (Map.Entry<String, StructType> entry : m_tables.entrySet()) {
            ArrayList<ArrayList<String>> fields = new ArrayList<>();
            for (StructField field : entry.getValue().getFields()) {
                ArrayList<String> f = new ArrayList<>();
                f.add(field.getName());
                f.add(getJavaTypeFromDataType(field.getDataType()).getCanonicalName());
                f.add("" + field.isNullable());
                fields.add(f);
            }
            String key = ensureQuotes(entry.getKey());
            m.put(key, fields);
        }
        config = config.withValue(TABLES_IDENTIFIER, ConfigValueFactory.fromMap(m));
        if (m_object != null) {
            config =  config.withValue(OBJECT_IDENTIFIER, ConfigValueFactory.fromAnyRef(ModelUtils.toString(m_object)));
        }
        return config.root().render(ConfigRenderOptions.concise());

    }

    /**
     * ensure that the given string starts and ends with quotes
     *
     * @param aString
     * @return original string if it is already in quotes, quoted string otherwise
     */
    private String ensureQuotes(final String aString) {
        if (!aString.startsWith("\"") || !aString.endsWith("\"")) {
            return "\"" + aString + "\"";
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
     * @return job result tables, may be empty map, but never null (map is immutable)
     */
    @Nonnull
    public Map<String, StructType> getTables() {
        return m_tables;
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
}
