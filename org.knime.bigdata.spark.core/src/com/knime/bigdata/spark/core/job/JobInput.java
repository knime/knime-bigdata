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
 *   Created on 28.01.2016 by koetter
 */
package com.knime.bigdata.spark.core.job;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Abstract class from which to derive classes that hold job-specific input parameters. Essentially, this class wraps a
 * {@link Map} from String keys ("parameters") to objects ("parameter values"). Everything put into the map will be
 * shipped to the Spark job, usually via serialization and network transfer. To achieve that behavior, subclasses of
 * this class must use the protected setter and getter methods (e.g. {@link #set(String, Object)} and
 * {@link #get(String)}) to define the input parameters for the Spark job.
 *
 * <p>
 * Parameter values /MUST/ be limited to <strong>small</strong> amounts of data (<10MB) (for details see
 * {@link JobData}). If you need to ship large amounts of data to your Spark job, please transfer that data
 * separately (e.g. using {@link JobWithFilesRun}).
 * </p>
 *
 * <p>
 * Parameter values /SHOULD/ be limited to JSON-compatible types (for details see {@link JobData}) for efficiency reasons. If you need to
 * use non-JSON-compatible types they /MUST/ by {@link Serializable}.
 * </p>
 *
 * <p>
 * As most Spark jobs consume one or more RDD/DataFrame and create one or more new ones, this class also provides
 * support for declaring "named objects", such as RDDs/DataFrames. This declaration makes it possible to have KNIME
 * automatically verify the existence of input RDDs/DataFrames prior to Spark job execution and manage the lifecycle of
 * these objects, e.g. deleting them when a KNIME workflow is reset etc.
 * </p>
 *
 * @see JobData
 * @see JobWithFilesRun
 *
 * @author Tobias Koetter, Bjoern Lohrmann KNIME.com
 */
@SparkClass
public abstract class JobInput extends JobData {

    private final static String KEY_PREFIX = "in";

    private final static String PARAM_PREFIX = "param.";

    private final static String NAMED_INPUT_OBJECTS_KEY = "namedInputObjects";

    private final static String NAMED_OUTPUT_OBJECTS_KEY = "namedOutputObjects";

    /**
     * Creates an instance of this class with an empty map.
     */
    protected JobInput() {
        super(KEY_PREFIX);
    }

    /**
     * @param paramName
     * @return {@link Integer} value
     */
    @Override
    protected Integer getInteger(final String paramName) {
        return super.getInteger(PARAM_PREFIX + paramName);
    }

    /**
     * @param paramName
     * @return {@link Long} value
     */
    @Override
    protected Long getLong(final String paramName) {
        return super.getLong(PARAM_PREFIX + paramName);
    }

    /**
     * @param paramName
     * @return {@link Double} value
     */
    @Override
    protected Double getDouble(final String paramName) {
        return super.getDouble(PARAM_PREFIX + paramName);
    }

    /**
     * @param paramName
     * @return the {@link Number} value
     */
    @Override
    protected Number getNumber(final String paramName) {
        return super.getNumber(PARAM_PREFIX + paramName);
    }

    /**
     * Retrieves the value of an input parameter from the underlying map. This method tries to cast the return object to
     * the generic type parameter T (beware of {@link ClassCastException}).
     *
     * @param paramName Name of an input parameter
     * @return value for the given input parameter
     */
    @Override
    protected <T> T get(final String paramName) {
        return super.get(PARAM_PREFIX + paramName);
    }

    /**
     * Retrieves the value of an input parameter from the underlying map. This method tries to cast the return object to
     * the generic type parameter T (beware of {@link ClassCastException}).
     *
     * @param paramName Name of an input parameter
     * @param defaultValue Default value for the input parameter
     * @return value for the given input parameter
     */
    @Override
    protected <T> T getOrDefault(final String paramName, final T defaultValue) {
        return super.getOrDefault(PARAM_PREFIX + paramName, defaultValue);
    }

    /**
     * Checks whether this instance contains a parameter with the given name.
     *
     * @param paramName parameter name, without the ParameterConstants.PARAM_INPUT prefix
     * @return true if there is such an input parameter
     */
    @Override
    protected boolean has(final String paramName) {
        return super.has(PARAM_PREFIX + paramName);
    }

    /**
     * Puts the given parameter value into the underlying map using the specified parameter name. Please note the
     * implicit type restrictions on the parameter value (only types supported by JSON are allowed) as described in the
     * javadoc of this class.
     *
     * @param paramName
     * @param paramValue Input parameter value. Must be JSON compatible.
     */
    @Override
    protected <T> T set(final String paramName, final T paramValue) {
        return super.set(PARAM_PREFIX + paramName, paramValue);
    }


    /**
     * Retrieves a list of named objects (e.g. RDDs) that the Spark job needs as input.
     *
     * @return a list of strings, where each string is the name of a named object (e.g. RDD).
     */
    public List<String> getNamedInputObjects() {
        return super.getOrDefault(NAMED_INPUT_OBJECTS_KEY, Collections.<String>emptyList());
    }

    /**
     * @return the ID of the first named input object (e.g. RDD) that a Spark job needs as input.
     * @throws IndexOutOfBoundsException If there are no named input objects
     */
    public String getFirstNamedInputObject() {
        return getNamedInputObjects().get(0);
    }

    /**
     * Specifies that the Spark job /requires/ the given named object (RDD, dataframe, etc). Its existence is then
     * verified prior to the execution of the Spark job.
     *
     * @param name
     */
    public void addNamedInputObject(final String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Named input object name must not be empty");
        }
        List<String> namedInputObjects = super.getOrDefault(NAMED_INPUT_OBJECTS_KEY, new LinkedList<String>());
        namedInputObjects.add(name);
        super.set(NAMED_INPUT_OBJECTS_KEY, namedInputObjects);
    }

    /**
     * Specifies that the Spark job /requires/ the given named objects (RDDs, dataframes, etc). Their existence is then
     * verified prior to the execution of the Spark job.
     *
     * @param names
     */
    public void addNamedInputObjects(final Collection<String> names) {
        List<String> namedInputObjects = super.getOrDefault(NAMED_INPUT_OBJECTS_KEY, new LinkedList<String>());
        namedInputObjects.addAll(names);
        super.set(NAMED_INPUT_OBJECTS_KEY, namedInputObjects);
    }

    /**
     * Retrieves a list of named objects (e.g. RDDs) that a Spark job shall create.
     *
     * @return a list of strings, where each string is the (to-be-created) name of a named object (e.g. RDD).
     */
    public List<String> getNamedOutputObjects() {
        return super.getOrDefault(NAMED_OUTPUT_OBJECTS_KEY, Collections.<String>emptyList());
    }

    /**
     * @return <code>true</code> if a output object is present otherwise <code>false</code>
     */
    public boolean hasFirstNamedOutputObject() {
        return !getNamedOutputObjects().isEmpty();
    }

    /**
     * @return the ID of the first named output object (e.g. RDD) that a Spark job shall create.
     * @throws IndexOutOfBoundsException If there are no named output objects
     */
    public String getFirstNamedOutputObject() {
        return getNamedOutputObjects().get(0);
    }

    /**
     * Specifies that the Spark job should produce the given named object (RDD, dataframe, etc).
     *
     * @param name
     */
    public void addNamedOutputObject(final String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Named output object name must not be empty");
        }
        List<String> namedOutputObjects = super.getOrDefault(NAMED_OUTPUT_OBJECTS_KEY, new LinkedList<String>());
        namedOutputObjects.add(name);
        super.set(NAMED_OUTPUT_OBJECTS_KEY, namedOutputObjects);
    }

    /**
     * Specifies that the Spark job should produce the given named objects (RDDs, dataframes, etc).
     *
     * @param names
     */
    public void addNamedOutputObjects(final Collection<String> names) {
        List<String> namedOutputObjects = super.getOrDefault(NAMED_OUTPUT_OBJECTS_KEY, new LinkedList<String>());
        namedOutputObjects.addAll(names);
        super.set(NAMED_OUTPUT_OBJECTS_KEY, namedOutputObjects);
    }

    @SuppressWarnings("unchecked")
    public <T extends JobInput> T withNamedInputObject(final String name) {
        addNamedInputObject(name);
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public <T extends JobInput> T withNamedInputObjects(final Collection<String> names) {
        addNamedInputObjects(names);
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public <T extends JobInput> T withNamedOutputObject(final String name) {
        addNamedOutputObject(name);
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public <T extends JobInput> T withNamedOutputObjects(final Collection<String> names) {
        addNamedOutputObjects(names);
        return (T) this;
    }
}
