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
 *   Created on Mar 8, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.sparkjobserver.request;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.sparkjobserver.request.ParsedResponse.FailureReason;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.Pair;

/**
 * The Spark jobserver reports errors quite inconsistently (different ways of indicating failure cause, in some failure
 * situations it even returns HTTP 200/OK). This class provides a heuristic to determine for each jobserver response,
 * whether it indicates a success or a failure (and which type of failure).
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JobserverResponseParser {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(JobserverResponseParser.class);

    /**
     * Tries to parse the jobserver response into a meaningful result.
     *
     * @param httpStatusCode
     * @param stringResponse
     * @return the parsed response as a {@link ParsedResponse}
     */
    public static ParsedResponse parseResponse(final int httpStatusCode,
        final String stringResponse) {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Rest response code:" + httpStatusCode + " value:" + stringResponse);
        }
        final JsonStructure jsonResponse = tryToParseAsJson(stringResponse);

        if (isDefinitelyFailure(httpStatusCode, jsonResponse)) {
            return handleDefiniteFailure(httpStatusCode, stringResponse, jsonResponse);
        } else {
            return handleProbableSuccess(httpStatusCode, stringResponse, jsonResponse);
        }
    }

    private static ParsedResponse handleDefiniteFailure(final int httpStatusCode, final String stringResponse,
        final JsonStructure jsonResponse) {

        // handle redirect
        if (httpStatusCode >= 300 && httpStatusCode < 400) {
            return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.REDIRECT);
        }

        switch(httpStatusCode) {
            case 401:
                if (stringResponse.equals("The resource requires authentication, which was not supplied with the request")) {
                    return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.AUTHENTICATION_REQUIRED);
                } else if (stringResponse.equals("The supplied authentication is invalid")) {
                    return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.AUTHENTICATION_FAILED);
                } else {
                    // *unknown* auth error situation. If we find those they should be incorporated into the known ones.
                    return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.UNPARSEABLE_RESPONSE);
                }
            case 407:
                return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.PROXY_AUTHENTICATION_REQUIRED);
            case 413:
                return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.ENTITY_TOO_LARGE);
        }

        // now, we must not check for http status codes anymore because jobserver has failures
        // that return a 200 code.
        if (jsonResponse != null && jsonResponse.getValueType() == ValueType.OBJECT) {
            // This matches with all *known* error situations that produce a JSON object with "status" and "result" in it
            return determineFailureReason(httpStatusCode, stringResponse, (JsonObject)jsonResponse);
        } else {
            // This covers all *unknown* error situations. If we find those they should be incorporated into the known ones.
            return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.UNPARSEABLE_RESPONSE);
        }
    }

    private static boolean isDefinitelyFailure(final int httpStatusCode, final JsonStructure jsonResponse) {
        return httpStatusCode >= 300
            // there are two error cases where the jobserver responds with
            // httpStatusCode 200 and { "status" = "ERROR", "result" = { <throwable> }}
            || (jsonResponse != null && jsonResponse.getValueType() == ValueType.OBJECT
                && ((JsonObject)jsonResponse).containsKey("status")
                && ((JsonObject)jsonResponse).getString("status").equals("ERROR"));
    }

    private static ParsedResponse handleProbableSuccess(final int httpStatusCode, final String stringResponse,
        final JsonStructure jsonResponse) {

        if (stringResponse.equals("OK")) {
            return ParsedResponse.createSuccess(httpStatusCode, stringResponse);
        }

        if (jsonResponse != null) {
            return ParsedResponse.createSuccess(httpStatusCode, stringResponse, jsonResponse);
        } else {
            return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.UNPARSEABLE_RESPONSE);
        }
    }

    private static ParsedResponse determineFailureReason(final int httpStatusCode, final String stringResponse,
        final JsonObject jsonObject) {

        if ((!jsonObject.containsKey("status")) || (!jsonObject.containsKey("result"))) {
            return null;
        }

        String status = jsonObject.getString("status");
        JsonValue result = jsonObject.get("result");

        // first try to match with known error status
        ParsedResponse errorResult;
        if ((errorResult = tryToMatchErrorStatus(httpStatusCode, stringResponse, status, result)) != null) {
            return errorResult;
        }

        // okay, 'status' was inconclusive. Maybe there is a throwable in the result?
        if (isThrowable(result)) {
            return ParsedResponse.createFailure(httpStatusCode, stringResponse, restoreThrowable((JsonObject)result));
        }

        // okay, 'status' was inconclusive and there is no throwable. Maybe 'result' is a string message that we know?
        if (result.getValueType() == ValueType.STRING) {
            String resultString = ((JsonString) result).getString();

            errorResult = tryToMatchErrorString(httpStatusCode, stringResponse, resultString);

            if (errorResult != null) {
                return errorResult;
            } else {
                return ParsedResponse.createFailure(httpStatusCode, stringResponse, resultString);
            }
        }

        // okay, 'status' was inconclusive, no throwable, no string message -> surrender
        return ParsedResponse.createFailure(httpStatusCode, stringResponse, FailureReason.UNPARSEABLE_RESPONSE);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Pair<Predicate<String>, FailureReason>[] STRING_RESULT_TO_REASON = new Pair[]{
        new Pair(Pattern.compile("appname .+ not found", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.JOB_APPID_NOT_FOUND),
        new Pair(Pattern.compile("cannot parse config: .+", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.JOB_CANNOT_PARSE_CONFIG),
        new Pair(Pattern.compile("classpath .+ not found", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.JOB_CLASSPATH_NOT_FOUND),
        new Pair(Pattern.compile("context name must start with letters", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.CONTEXT_NAME_MUST_START_WITH_LETTERS),
        new Pair(Pattern.compile("context .+ exists", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.CONTEXT_ALREADY_EXISTS),
        new Pair(Pattern.compile("context .+ not found", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.CONTEXT_NOT_FOUND),
        new Pair(Pattern.compile("invalid job type for this context", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.JOB_TYPE_INVALID),
        new Pair(Pattern.compile("jar is not of the right format", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.JAR_INVALID),
        new Pair(Pattern.compile("no running job with ID", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.JOB_NOT_RUNNING_ANYMORE),
        new Pair(Pattern.compile("no such job ID .+", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.JOB_NOT_FOUND),
        new Pair(Pattern.compile("request timed out. try using .+", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.REQUEST_TIMEOUT),
        new Pair(Pattern.compile("unable to delete data file .+", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.DATAFILE_DELETION_FAILED),
        new Pair(Pattern.compile("failed to store data file .+", Pattern.CASE_INSENSITIVE).asPredicate(),
            FailureReason.DATAFILE_STORING_FAILED)};

    private static ParsedResponse tryToMatchErrorString(final int httpStatusCode, final String stringResponse, final String stringResult) {
        for (Pair<Predicate<String>, FailureReason> patternReasonPair : STRING_RESULT_TO_REASON) {
            if (patternReasonPair.getFirst().test(stringResult)) {
                return ParsedResponse.createFailure(httpStatusCode, stringResponse, patternReasonPair.getSecond());
            }
        }
        return null;
    }

    private static final HashMap<String, FailureReason> ERROR_STATUS_TO_REASON = new HashMap<String, FailureReason>();

    static {
        ERROR_STATUS_TO_REASON.put("CONTEXT INIT ERROR", FailureReason.CONTEXT_INIT_FAILED);
        ERROR_STATUS_TO_REASON.put("VALIDATION FAILED", FailureReason.JOB_VALIDATION_FAILED);
        ERROR_STATUS_TO_REASON.put("JOB LOADING FAILED", FailureReason.JOB_LOADING_FAILED);
        ERROR_STATUS_TO_REASON.put("CONTEXT INIT FAILED", FailureReason.CONTEXT_INIT_FAILED);
        ERROR_STATUS_TO_REASON.put("NO SLOTS AVAILABLE", FailureReason.JOB_NO_FREE_SLOTS_AVAILABLE);
    }

    private static ParsedResponse tryToMatchErrorStatus(final int httpStatusCode, final String stringResponse,
        final String status, final JsonValue result) {

        if (ERROR_STATUS_TO_REASON.containsKey(status)) {
            if (isThrowable(result)) {
                return ParsedResponse.createFailure(httpStatusCode, stringResponse, ERROR_STATUS_TO_REASON.get(status), restoreThrowable((JsonObject)result));
            } else {
                return ParsedResponse.createFailure(httpStatusCode, stringResponse, ERROR_STATUS_TO_REASON.get(status));
            }
        } else {
            return null;
        }
    }

    private static JsonStructure tryToParseAsJson(final String stringResponse) {
        try {
            return Json.createReader(new StringReader(stringResponse)).read();
        } catch (JsonException e) {
            return null;
        }
    }

    private static boolean isThrowable(final JsonValue result) {
        if (result.getValueType() != ValueType.OBJECT) {
            return false;
        }

        JsonObject resultObject = (JsonObject)result;
        return resultObject.containsKey("message") && resultObject.containsKey("errorClass")
            && resultObject.containsKey("stack");
    }

    private static RestoredThrowable restoreThrowable(final JsonObject resultObject) {

        String errorMessage = "No error message provided";
        if (!resultObject.isNull("message")
              && !resultObject.getString("message").isEmpty()
              && !resultObject.getString("message").equals("null")) {

            errorMessage = resultObject.getString("message");
        }

        String errorClass = resultObject.getString("errorClass");
        RestoredThrowable t = new RestoredThrowable(errorClass, errorMessage);

        // try to reconstruct stack trace from json
        Pattern traceLinePattern = Pattern.compile("^(.+)\\.([^(]+)\\(([^:]+)\\:(\\d+)\\)$");

        List<StackTraceElement> stackElems = new ArrayList<StackTraceElement>();

        for (JsonValue stackElem : resultObject.getJsonArray("stack")) {
            Matcher m = traceLinePattern.matcher(((JsonString)stackElem).getString());
            if (m.matches()) {
                stackElems.add(new StackTraceElement(m.group(1), m.group(2), m.group(3), Integer.parseInt(m.group(4))));
            } else {
                stackElems.add(new StackTraceElement("<parseError>", "<parseError>", "<parseError>", 0));
            }
        }
        t.setStackTrace(stackElems.toArray(new StackTraceElement[0]));
        return t;
    }
}
