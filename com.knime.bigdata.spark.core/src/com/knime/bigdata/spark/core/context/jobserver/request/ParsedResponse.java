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
 *   Created on Mar 7, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context.jobserver.request;

import javax.json.JsonStructure;
import javax.json.JsonValue.ValueType;

/**
 * Value class to hold the result of parsing a jobserver response. Instances of this class should be created using
 * {@link JobserverResponseParser#parseResponse(int, String)}.
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class ParsedResponse {

    public enum FailureReason {
            // /////////////////////// General errors ///////////////////////
            /**
             * Indicates that the error cause is unknown, but the server response contained a Throwable. {@link RequestResults}
             * with this failure reason contain a throwable, see {@link RequestResult#createFailure(Throwable)}.
             */
            THROWABLE,

            /**
             * Indicates that the error cause is unknown, but the server response contained a String message.
             * {@link RequestResults} with this failure reason contain a custom message, see
             * {@link RequestResult#createFailure(String)}.
             */
            UNKNOWN,

            /**
             * Indicates that the server response contained no usable information. Quite likely, if this happens
             * this is a bug in the parser.
             */
            UNPARSEABLE_RESPONSE,

            /**
             * Indicates that the request timed out on the server side.
             */
            REQUEST_TIMEOUT,

            /**
             * Indicates that the server returned a HTTP 3xx code (redirect).
             */
            REDIRECT,

            // /////////////////////// HTTP 401/407: Authentication related errors ///////////////////////
            AUTHENTICATION_REQUIRED,
            AUTHENTICATION_FAILED,
            PROXY_AUTHENTICATION_REQUIRED,

            // /////////////////////// HTTP 413 ///////////////////////
            ENTITY_TOO_LARGE,

            // /////////////////////// Known errors returned by /context ///////////////////////
            CONTEXT_NAME_MUST_START_WITH_LETTERS,
            CONTEXT_ALREADY_EXISTS,
            CONTEXT_NOT_FOUND,
            /**
             * Indicates that the context could not be created. {@link RequestResults}
             * with this failure reason contain a throwable, see {@link RequestResult#createFailure(Throwable)}.
             */
            CONTEXT_INIT_FAILED,

            // /////////////////////// Known errors returned by /jar ///////////////////////
            JAR_INVALID,

            // /////////////////////// Known errors returned by /data ///////////////////////
            DATAFILE_DELETION_FAILED,
            DATAFILE_STORING_FAILED,

            // /////////////////////// Known errors returned by /job ///////////////////////
            // Also: CONTEXT_NOT_FOUND
            JOB_APPID_NOT_FOUND,
            JOB_CLASSPATH_NOT_FOUND,
            JOB_LOADING_FAILED,
            JOB_TYPE_INVALID,
            JOB_NO_FREE_SLOTS_AVAILABLE,
            JOB_CANNOT_PARSE_CONFIG,
            JOB_VALIDATION_FAILED,
            JOB_NOT_RUNNING_ANYMORE,
            JOB_NOT_FOUND;
    }

    private final int m_httpResponseStatus;

    private final String m_httpResponseEntity;

    private final FailureReason m_failureReason;

    private final Throwable m_throwable;

    private final String m_customErrorMessage;

    private final JsonStructure m_jsonBody;

    private final boolean m_hasPlaintextBody;

    /**
     * Private constructor only for use by factory methods createFailure() and createSuccess().
     */
    private ParsedResponse(final int httpResponseStatus,
        final String httpResponseEntity,
        final FailureReason reason,
        final Throwable throwable,
        final String customErrorMessage,
        final JsonStructure jsonBody,
        final boolean hasPlaintextBody) {

        this.m_httpResponseStatus = httpResponseStatus;
        this.m_httpResponseEntity = httpResponseEntity;
        this.m_failureReason = reason;
        this.m_throwable = throwable;
        this.m_customErrorMessage = customErrorMessage;
        this.m_jsonBody = jsonBody;
        this.m_hasPlaintextBody = hasPlaintextBody;
    }

    private ParsedResponse(final int httpResponseStatus,
        final String httpResponseEntity, final JsonStructure json) {

        this(httpResponseStatus, httpResponseEntity, null, null, null, json, false);
    }


    private ParsedResponse(final int httpResponseStatus,
        final String httpResponseEntity, final boolean hasPlaintextBody) {
        this(httpResponseStatus, httpResponseEntity, null, null, null, null, hasPlaintextBody);
    }

    /**
     * @return whether this parsed response contains a failure or a success.
     */
    public boolean isFailure() {
        return m_failureReason != null;
    }

    /**
     * @return the HTTP response status code
     */
    public int getHttpResponseStatus() {
        return m_httpResponseStatus;
    }

    /**
     * @return the reason
     */
    public FailureReason getFailureReason() {
        return m_failureReason;
    }

    /**
     * @return the throwable
     */
    public Throwable getThrowable() {
        return m_throwable;
    }

    /**
     * @return the customErrorMessage
     */
    public String getCustomErrorMessage() {
        return m_customErrorMessage;
    }

    /**
     * @return the jsonBody
     */
    public JsonStructure getJsonBody() {
        return m_jsonBody;
    }

    /**
     * @return the plaintextBody
     */
    public String getPlaintextBody() {
        return m_httpResponseEntity;
    }

    /**
     * @return the response entity
     */
    public Object getResponseEntity() {
        return m_httpResponseEntity;
    }


    /**
     *
     * @return whether the request result contains a plaintext body
     */
    public boolean hasPlaintextBody() {
        return this.m_hasPlaintextBody;
    }

    /**
     *
     * @return whether the request result contains a json object as body
     */
    public boolean hasJsonObjectBody() {
        return (this.m_jsonBody != null) && (this.m_jsonBody.getValueType() == ValueType.OBJECT);
    }

    /**
    *
    * @return whether the request result contains a json object as body
    */
   public boolean hasJsonObjectArray() {
       return (this.m_jsonBody != null) && (this.m_jsonBody.getValueType() == ValueType.OBJECT);
   }

   /**
    * Creates a response indicating failure with a given reason.
    *
    * @param httpResponseStatus
    * @param httpResponseEntity
    * @param reason
    * @return a parsed response instance
    */
    public static ParsedResponse createFailure(final int httpResponseStatus,
        final String httpResponseEntity, final FailureReason reason) {
        return new ParsedResponse(httpResponseStatus, httpResponseEntity, reason, null, null, null, false);
    }

    /**
     * Creates a response indicating failure with a given reason and throwable.
     *
     * @param httpResponseStatus
     * @param httpResponseEntity
     * @param reason
     * @param throwable
     * @return a parsed response instance
     */
    public static ParsedResponse createFailure(final int httpResponseStatus,
        final String httpResponseEntity, final FailureReason reason, final Throwable throwable) {

        return new ParsedResponse(httpResponseStatus, httpResponseEntity, reason, throwable, null, null, false);
    }

    /**
     * Creates a response indicating failure with reason {@link FailureReason#THROWABLE} and the throwable. This is a fallback if
     * the real failure reason could not be determined but we have a stack trace.
     *
     * @param httpResponseStatus
     * @param httpResponseEntity
     * @param throwable
     * @return a parsed response instance
     */
    public static ParsedResponse createFailure(final int httpResponseStatus,
        final String httpResponseEntity, final Throwable throwable) {
        return new ParsedResponse(httpResponseStatus, httpResponseEntity, FailureReason.THROWABLE, throwable, null, null, false);
    }

    /**
     * Creates a response indicating failure with reason {@link FailureReason#UNKNOWN} but a custom message. This is a fallback if
     * the real failure reason could not be determined but we have a message.
     *
     * @param httpResponseStatus
     * @param httpResponseEntity
     * @param customMessage
     * @return a parsed response instance
     */
    public static ParsedResponse createFailure(final int httpResponseStatus,
        final String httpResponseEntity, final String customMessage) {
        return new ParsedResponse(httpResponseStatus, httpResponseEntity, FailureReason.UNKNOWN, null, customMessage, null, false);
    }

    /**
     * Creates a response indicating success with a JSON response.
     *
     * @param httpResponseStatus
     * @param httpResponseEntity
     * @param json
     * @return a parsed response instance
     */
    public static ParsedResponse createSuccess(final int httpResponseStatus,
        final String httpResponseEntity, final JsonStructure json) {
        return new ParsedResponse(httpResponseStatus, httpResponseEntity, null, null, null, json, false);
    }

    /**
     * Creates a response indicating success with a plaintext body (httpResponseEntity will be taken as plaintext body).
     *
     * @param httpResponseStatus
     * @param httpResponseEntity
     * @return a parsed response instance
     */
    public static ParsedResponse createSuccess(final int httpResponseStatus,
        final String httpResponseEntity) {
        return new ParsedResponse(httpResponseStatus, httpResponseEntity, null, null, null, null, true);
    }
}
