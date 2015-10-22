package com.knime.bigdata.spark.jobserver.client;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.stream.JsonParsingException;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;

/**
 *
 * utility for the construction of JSON strings from Object arrays
 *
 * @author dwk
 *
 */
public class JsonUtils {

    /**
     * convert the given Object to a JSon string
     *
     * @param aKeyValuePairs
     * @return json string
     */
    public static String asJson(final Object aKeyValuePairs) {
        if (aKeyValuePairs == null) {
            return "";
        }
        if (!(aKeyValuePairs instanceof Object[])) {
            return "\"" + aKeyValuePairs.toString() + "\"";
        }
        Object[] keyValuePairs = (Object[])aKeyValuePairs;
        if (keyValuePairs.length < 2) {
            return "";
        }
        // first elem must be key
        final StringBuilder sb = new StringBuilder("\"" + keyValuePairs[0].toString() + "\" :");
        // second element can be String or Array
        if (keyValuePairs[1] instanceof Object[]) {
            sb.append("{");
            sb.append(asJson(keyValuePairs[1]));
            sb.append("}");
        } else if (keyValuePairs[1] == null) {
            sb.append("null");
        } else {
            String val = keyValuePairs[1].toString();
            if (!val.startsWith("[")) {
                sb.append("\"");
            }
            sb.append(val);
            if (!val.startsWith("[")) {
                sb.append("\"");
            }
        }
        if (keyValuePairs.length > 2) {
            sb.append(",");
            Object[] tail = new Object[keyValuePairs.length - 2];
            System.arraycopy(keyValuePairs, 2, tail, 0, tail.length);
            sb.append(asJson(tail));
        }
        return sb.toString();
    }

    /**
     * convert the given array of element to a JSon string that represents an array
     *
     * @param aElems
     * @return json string representing an array
     */
    public static String toJsonArray(final Object... aElems) {
        final StringBuilder sb = new StringBuilder();
        if (aElems != null) {
            sb.append("[");
            for (int i = 0; i < aElems.length; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                if (aElems[i] == null) {
                    sb.append("null");
                } else if (!(aElems[i] instanceof Object[])) {
                    sb.append("\"" + aElems[i].toString() + "\"");
                } else {
                    sb.append("{");
                    sb.append(asJson(aElems[i]));
                    sb.append("}");
                }

            }
            sb.append("]");
        }
        return sb.toString();
    }

    /**
     * convert the given array of element to a JSon string that represents an array
     *
     * @note String are URLEncoded (UTF-8) and must be decoded again!
     * @param aElems
     * @return json string representing an array public static String toJson2DimArray(final Object[][] aElems) { final
     *         StringBuilder sb = new StringBuilder("["); for (int i = 0; i < aElems.length; i++) { if (i > 0) {
     *         sb.append(","); } sb.append("["); for (int j = 0; j < aElems[i].length; j++) { if (j > 0) {
     *         sb.append(","); } if (aElems[i][j] == null) { sb.append("null"); } else if (aElems[i][j] instanceof
     *         String) { try { sb.append("\"" + URLEncoder.encode(aElems[i][j].toString(), "UTF-8") + "\""); } catch
     *         (UnsupportedEncodingException e) { sb.append("null"); } } else { sb.append("\"" + aElems[i][j].toString()
     *         + "\""); } } sb.append("]"); } sb.append("]"); return sb.toString(); }
     */

    /**
     * convert the given string representation of a Json array to a JsonArray object
     *
     * @param aJsonArrayString
     * @return the parse JsonArray
     * @throws GenericKnimeSparkException
     */
    public static JsonArray toJsonArray(final String aJsonArrayString) throws GenericKnimeSparkException {
        try {
            return Json.createReader(new StringReader(aJsonArrayString)).readArray();
        } catch (JsonParsingException e) {
            if (aJsonArrayString != null && aJsonArrayString.equals("The supplied authentication is invalid")) {
                throw new GenericKnimeSparkException("Could not login to server: " + aJsonArrayString, e);
            }
            throw new GenericKnimeSparkException("Failed to parse server response: " + aJsonArrayString, e);
        }
    }

    /**
     * @param sql the sql string to cleanup
     * @return the cleaned up sql string
     */
    public static String cleanupSQL(final String sql) {
        //TODO: Provide a (un)escape function for strings for client and server side
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        //replace all new lines with spaces
        return sql.replaceAll("\\n", " ");
    }
}