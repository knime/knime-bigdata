package com.knime.bigdata.spark.jobserver.client;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;

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
            sb.append("\"\"");
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
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < aElems.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            if (aElems instanceof Integer[]) {
                sb.append(aElems[i].toString());
            }
            else if (!(aElems[i] instanceof Object[])) {
                sb.append( "\"" + aElems[i].toString() + "\"");
            } else {
                sb.append("{");
                sb.append(asJson(aElems[i]));
                sb.append("}");
            }

        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * convert the given array of element to a JSon string that represents an array
     *
     * @param aElems
     * @return json string representing an array
     */
    public static String toJson2DimArray(final Object[][] aElems) {
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < aElems.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("[");
            for (int j = 0; j < aElems[i].length; j++) {
                if (j > 0) {
                    sb.append(",");
                }
                sb.append( "\"" + aElems[i][j].toString() + "\"");
            }
            sb.append("]");
        }
        sb.append("]");
        return sb.toString();
    }
    /**
     * convert the given string representation of a Json array to a JsonArray object
     * @param aJsonArrayString
     * @return the parse JsonArray
     */
    public static JsonArray toJsonArray(final String aJsonArrayString) {
	    return Json.createReader(new StringReader(aJsonArrayString)).readArray();
	}
}
