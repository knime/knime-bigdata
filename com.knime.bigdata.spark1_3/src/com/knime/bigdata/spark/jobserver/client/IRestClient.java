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
 *   Created on 22.05.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.client;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 *
 * @author dwk
 */
interface IRestClient {

    /**
     * check the status of the given response
     *
     * @param response response to check
     * @param jobClassName the {@link KnimeSparkJob} that belongs to the response
     * @param aJsonParams the {@link JobConfig} of the job
     * @throws GenericKnimeSparkException if the status is not ok
     */
    void checkJobStatus(Response response, String jobClassName, String aJsonParams)
            throws GenericKnimeSparkException;

    /**
     * check the status of the given response
     *
     * @param response response to check
     * @param aErrorMsg error message prefix in case the response is not in one of the expected stati
     * @param aStatus array of expected stati that are OK, all other response stati will cause an exception
     * @throws GenericKnimeSparkException
     */
    void checkStatus(Response response, String aErrorMsg, Status... aStatus) throws GenericKnimeSparkException;

    <T> Response post(final KNIMESparkContext aContextContainer, final String aPath, final String[] aArgs, final Entity<T> aEntity) throws GenericKnimeSparkException;

    Response delete(final KNIMESparkContext aContextContainer, final String aPath) throws GenericKnimeSparkException;

    /**
     * send the given type of command to the REST server and convert the result to a JSon array
     *
     * @param aContextContainer context configuration container
     * @param aType
     * @return JSonArray with result
     * @throws GenericKnimeSparkException
     */
    JsonArray toJSONArray(final KNIMESparkContext aContextContainer, final String aType) throws GenericKnimeSparkException;

    /**
     * send the given type of command to the REST server and convert the result to a JSon object
     *
     * @param aContextContainer context configuration container
     * @param aType
     * @return JsonObject with result
     * @throws GenericKnimeSparkException
     */
    JsonObject toJSONObject(final KNIMESparkContext aContextContainer, final String aType) throws GenericKnimeSparkException;

    /**
     * return the string value of the given field / sub-field combination from the given response
     *
     * @param response
     * @param aField
     * @param aSubField
     * @return String value
     * @throws GenericKnimeSparkException
     */
    String getJSONFieldFromResponse(final Response response, final String aField, final String aSubField)
        throws GenericKnimeSparkException;
}