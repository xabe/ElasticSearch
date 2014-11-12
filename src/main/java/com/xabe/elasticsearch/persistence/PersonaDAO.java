package com.xabe.elasticsearch.persistence;

import java.util.List;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;

import com.xabe.elasticsearch.modelo.Persona;

public interface PersonaDAO {
	
	IndexResponse create(Persona persona);
	
	DeleteResponse delete(String id);
	
	void update(String id, Persona persona) throws Exception;
	
	Persona search(String id);
	
	List<Persona> searchField(String filter,String value);

}
