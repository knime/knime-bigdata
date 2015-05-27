package com.knime.bigdata.spark.jobserver.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
//with parseBase64Binary() and printBase64Binary().
import java.util.logging.Logger;

//Java 8:
//import java.util.Base64;
//Java 7:
import javax.xml.bind.DatatypeConverter;

/**
 *
 * model serialization utility (until Spark support PMML import and export, hopefully with 1.4)
 */
public class ModelUtils {

	private final static Logger LOGGER = Logger.getLogger(ModelUtils.class
			.getName());

	/**
	 * de-serialize an object from the given Base64 string and cast it to the given type
     * @param aString object to be de-serialized
     * @return string representation of given object
	 */
	public static <T> T fromString(final String aString) {
		//JAva 1.8: byte[] data = Base64.getDecoder().decode(aString);
        byte[] data =  DatatypeConverter.parseBase64Binary(aString);
		try (final ObjectInputStream ois = new ObjectInputStream(
				new ByteArrayInputStream(data))) {
			return (T) ois.readObject();
		} catch (IOException | ClassNotFoundException e) {
			LOGGER.severe(e.getMessage());
			LOGGER.severe( "ERROR - de-serialization failed: "+e.getMessage());
			return null;
		}
	}

	/**
	 * serializes the given object to a Base64 string
	 * @param aObject object to be serialized
	 * @return string representation of given object
	 */
	public static String toString(final Serializable aObject)  {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try(final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
			oos.writeObject(aObject);
			//Java 1.8: return Base64.getEncoder().encodeToString(baos.toByteArray());
            return DatatypeConverter.printBase64Binary(baos.toByteArray());
		} catch (IOException e) {
			LOGGER.severe(e.getMessage());
			return "ERROR - serialization failed!"+e.getMessage();
		}
	}
}
