package com.xabe.elasticsearch.util;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;

public final class JsonUtils {

	private final static ObjectMapper mapper = new ObjectMapper();
	
	private JsonUtils() {}
	
	static {
		mapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
		mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,false);
	}

	public static String getJson(Object obj) throws IOException {
		String json = "";
		if (null == obj) 
		{
			throw new NullPointerException("El objecto no puede ser nulo");
		}
		json = mapper.writeValueAsString(obj);
		return json;
	}

	public static <T> T getPojo(String json, Class<T> classType)throws IOException {
		T result = null;
		if (null == json)
		{
			throw new NullPointerException("El objecto no puede ser nulo");
		}
		result = mapper.readValue(json, classType);
		return result;
	}
}
