package com.xabe.elasticsearch.client;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public final class ClientProvider {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClientProvider.class);
	private static ClientProvider instance;
	private static Object lock = new Object();
	private Node node;
	private ObjectMapper mapper;

	
	private ClientProvider() {
		mapper = new ObjectMapper();
		createNode();
	}
	
	public static ClientProvider getInstance() {
		if (instance == null) 
		{
			synchronized (lock) 
			{
				if (null == instance) 
				{
					instance = new ClientProvider();					
				}
			}
		}
		return instance;
	}

	private void createNode() {
		node = nodeBuilder().local(true).client(false).node();
		LOGGER.info("Creado el cluster de elasticsearch");
	}

	public void closeNode() {
		if (!node.isClosed())
		{			
			node.close();
			LOGGER.info("Cerrado el cluster de elasticsearch");
		}
	}

	public Client getClient() {
		return node.client();
	}
	
	public String getJson(Object value) throws NullPointerException {
		String json = "";
		try
		{
			if(null == value)
			{
				throw new NullPointerException("El objecto no puede ser nulo");
			}
			json =  mapper.writeValueAsString(value);
		}catch(JsonProcessingException exception){
			LOGGER.error("Error al parsear el object "+value.getClass().getName()+" a json" + exception.getMessage());
		}
		return json;
	}
	
	public <T> T getObject(String value, Class<T> classType) throws NullPointerException {
		T result = null;
		try
		{
			if(null == value)
			{
				throw new NullPointerException("El json no puede ser nulo");
			}
			result =  mapper.readValue(value, classType);
		}catch(IOException exception){
			LOGGER.error("Error al parsear el object "+value.getClass().getName()+" a json" + exception.getMessage());
		}
		return result;
	}
}
