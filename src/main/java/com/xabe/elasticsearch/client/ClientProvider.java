package com.xabe.elasticsearch.client;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClientProvider {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClientProvider.class);
	private static ClientProvider instance;
	private static Object lock = new Object();
	private Node node;

	private ClientProvider() {
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
}
