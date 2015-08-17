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
 *   Created on 04.08.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

//with parseBase64Binary() and printBase64Binary().
//Java 8:
//import java.util.Base64;
//Java 7:
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import com.typesafe.config.Config;

/**
 *
 * @author dwk
 */
public class JobConfig {

    private final Config m_config;

    final private String INPUT_PREFIX = ParameterConstants.PARAM_INPUT + ".";

    final private String OUTPUT_PREFIX = ParameterConstants.PARAM_OUTPUT + ".";

    /**
     * stores reference to given config
     *
     * @param aConfig
     */
    public JobConfig(final Config aConfig) {
        m_config = aConfig;
    }

    /**
     * checks whether config contains an input parameter with the given name
     *
     * @param aParamName parameter name, without the ParameterConstants.PARAM_INPUT prefix
     * @return true if there is such an input parameter
     */
    public boolean hasInputParameter(final String aParamName) {
        return m_config.hasPath(INPUT_PREFIX + aParamName);
    }

    /**
     * checks whether config contains an output parameter with the given name
     *
     * @param aParamName parameter name, without the ParameterConstants.PARAM_OUTPUT prefix
     * @return true if there is such an output parameter
     */
    public boolean hasOutputParameter(final String aParamName) {
        return m_config.hasPath(OUTPUT_PREFIX + aParamName);
    }

    /**
     *
     * @param aParamName parameter name, without the ParameterConstants.PARAM_OUTPUT prefix
     * @return String output parameter value
     */
    public String getOutputStringParameter(final String aParamName) {
        return m_config.getString(OUTPUT_PREFIX + aParamName);
    }

    /**
     * retrieve the value of a String parameter
     *
     * @param aParamName parameter name, without the ParameterConstants.PARAM_INPUT prefix
     * @return String value for the given input String parameter
     */
    public String getInputParameter(final String aParamName) {
        return m_config.getString(INPUT_PREFIX + aParamName);
    }

    /**
     *
     * retrieve the value of a parameter
     *
     * @param aParamName parameter name, without the ParameterConstants.PARAM_INPUT prefix
     * @param aType desired return type, needed as TypeSafe.Config has different access methods for the different types
     *            and we sometimes send numbers in quotes - so this is safer
     * @return value for the given input parameter
     */
    @SuppressWarnings("unchecked")
    public <T> T getInputParameter(final String aParamName, final Class<?> aType) {
        if (aType == Integer.class) {
            return (T)Integer.valueOf(m_config.getInt(INPUT_PREFIX + aParamName));
        }
        if (aType == Double.class) {
            return (T)Double.valueOf(m_config.getDouble(INPUT_PREFIX + aParamName));
        }
        if (aType == Boolean.class) {
            return (T)Boolean.valueOf(m_config.getBoolean(INPUT_PREFIX + aParamName));
        }
        if (aType == Long.class) {
            return (T)Long.valueOf(m_config.getLong(INPUT_PREFIX + aParamName));
        }

        return (T)getInputParameter(aParamName);

    }

    /**
     *
     * retrieve the value of a list parameter
     *
     * @param aParamName parameter name, without the ParameterConstants.PARAM_INPUT prefix
     * @param aType desired return type, needed as TypeSafe.Config has different access methods for the different types
     *            and we sometimes send numbers in quotes - so this is safer
     * @return list of values for the given input parameter
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getInputListParameter(final String aParamName, final Class<?> aType) {
        if (aType == Integer.class) {
            return (List<T>)m_config.getIntList(INPUT_PREFIX + aParamName);
        }
        if (aType == Double.class) {
            return (List<T>)m_config.getDoubleList(INPUT_PREFIX + aParamName);
        }
        if (aType == Boolean.class) {
            return (List<T>)m_config.getBooleanList(INPUT_PREFIX + aParamName);
        }
        if (aType == Long.class) {
            return (List<T>)m_config.getLongList(INPUT_PREFIX + aParamName);
        }
        return (List<T>)m_config.getStringList(INPUT_PREFIX + aParamName);
    }

    /**
     * de-serialize an object from the Base64 string parameter value and cast it to the given type
     *
     * @param aParamName input parameter name
     * @return string representation of given object
     * @throws GenericKnimeSparkException if T cannot be found or the data is corrupted
     */
    public <T> T decodeFromInputParameter(final String aParamName) throws GenericKnimeSparkException {
        return decodeFromParameter(INPUT_PREFIX + aParamName);
    }

    /**
     * de-serialize an object from the Base64 string parameter value and cast it to the given type
     *
     * @param aParamName parameter name
     * @return string representation of given object
     * @throws GenericKnimeSparkException if T cannot be found or the data is corrupted
     */
    @SuppressWarnings("unchecked")
    public <T> T decodeFromParameter(final String aParamName) throws GenericKnimeSparkException {
        //JAva 1.8: byte[] data = Base64.getDecoder().decode(aString);
        byte[] data = DatatypeConverter.parseBase64Binary(m_config.getString(aParamName));
        try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (T)ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new GenericKnimeSparkException(e);
        }
    }

    /**
     * serializes the given object to a Base64 string
     *
     * @param aObject object to be serialized
     * @return string representation of given object
     * @throws GenericKnimeSparkException if data is corrupted
     */
    public static String encodeToBase64(final Serializable aObject) throws GenericKnimeSparkException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(aObject);
            //Java 1.8: return Base64.getEncoder().encodeToString(baos.toByteArray());
            return DatatypeConverter.printBase64Binary(baos.toByteArray());
        } catch (IOException e) {
            throw new GenericKnimeSparkException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return m_config.toString();
    }
}
