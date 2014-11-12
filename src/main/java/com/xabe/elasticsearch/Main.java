package com.xabe.elasticsearch;

import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xabe.elasticsearch.client.ClientProvider;
import com.xabe.elasticsearch.modelo.Persona;
import com.xabe.elasticsearch.persistence.PersonaDAO;
import com.xabe.elasticsearch.persistence.impl.PersonaDAOImpl;
import com.xabe.elasticsearch.util.Constant;

public class Main {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	private static String printIndexResponse(IndexResponse response){
		String id;
		LOGGER.info("El indice : "+response.getIndex());
		LOGGER.info("El tipo : "+response.getType());
		LOGGER.info("El id : "+ (id = response.getId()));
		LOGGER.info("La version : "+ response.getVersion());
		return id;

	}
	
	public static void main(String[] args) {
		
		PersonaDAO dao = new PersonaDAOImpl();
		
		
		try
		{
			Persona persona = new Persona();
			persona.setName("chabir");
			persona.setSurname("Atrahocuh");
			persona.setTelephone(99999999);
			persona.setJob("Programdor");
			LOGGER.info("Creando la persona : "+persona.toString());
			
			//Create Persona
			IndexResponse response = dao.create(persona);
			LOGGER.info("Creado la persona en elasticsearch");
			String id = printIndexResponse(response);
			
			//Search una Persona			
			LOGGER.info("Buscamos la persona con el id " + id);
			persona = dao.search(id);
			LOGGER.info("La persona con el id "+id+" es: "+persona.toString());
			
			
			//Update una Persona
			LOGGER.info("Actulizamos la persona con el id " + id +" persona : "+persona.toString());
			LOGGER.info("La persona antigua : "+persona.toString());
			persona.setJob("Javero");
			LOGGER.info("La persona nueva   : "+persona.toString());
			dao.update(id, persona);
			LOGGER.info("La persona actulizada");
			
			
			
			//Creamos la segunda persona
			persona.setName("Pepe");
			persona.setSurname("Perez");
			LOGGER.info("La persona 2   : "+persona.toString());
			response = dao.create(persona);
			LOGGER.info("Creado la persona en elasticsearch");
			String id_2 = printIndexResponse(response);
			
			//Le damos tiempo a elasticsearch de actulizar
			Thread.currentThread().sleep(3000);
			
			//Buscamos a las personas con el nombre 
			List<Persona> personas = dao.searchField(Constant.FIELD_JOB, "Javero");
			LOGGER.info("Numero de personas encontradas: "+ personas.size());
			for(Persona aux: personas){
				LOGGER.info(aux.toString());
			}
				
			//Delete la Persona
			dao.delete(id);
			LOGGER.info("La persona borrada con el id : "+id);
			
			dao.delete(id_2);
			LOGGER.info("La persona borrada con el id : "+id_2);
			
			
		}catch(Exception e){
			LOGGER.error("Error : "+e.getMessage());
		}
		ClientProvider.getInstance().closeNode();
	}

}
