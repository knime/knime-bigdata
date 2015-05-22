package com.knime.bigdata.spark.jobserver.client;

/**
 *
 * utility for the construction of JSON strings from Object arrays
 * @author dwk
 *
 */
public class JsonUtils {

    /**
     * convert the given Object to a JSon string
     * @param aKeyValuePairs
     * @return jason string
     */
	public static String asJson(final Object aKeyValuePairs) {
		if (aKeyValuePairs == null) {
			return "";
		}
		if (!(aKeyValuePairs instanceof Object[])) {
			return "\"" + aKeyValuePairs.toString() + "\"";
		}
		Object[] keyValuePairs = (Object[]) aKeyValuePairs;
		if (keyValuePairs.length < 2) {
			return "";
		}
		// first elem must be key
		final StringBuilder sb = new StringBuilder("\""
				+ keyValuePairs[0].toString() + "\" :");
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

//	/**
//	 * convert the given array of element to a JSon string that represents an array
//	 * @param aElems
//	 * @return json string representing an array
//	 */
//	private static String toJsonArray(final Object[]... aElems) {
//		final StringBuilder sb = new StringBuilder("[");
//		for (int i = 0; i < aElems.length; i++) {
//			if (i > 0) {
//				sb.append(",");
//			}
//			sb.append("{");
//			sb.append(asJson(aElems[i]));
//			sb.append("}");
//		}
//		sb.append("]");
//		return sb.toString();
//	}

}
