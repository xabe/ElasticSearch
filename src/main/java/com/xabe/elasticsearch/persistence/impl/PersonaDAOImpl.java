package com.xabe.elasticsearch.persistence.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.xabe.elasticsearch.client.ClientProvider;
import com.xabe.elasticsearch.modelo.Persona;
import com.xabe.elasticsearch.persistence.PersonaDAO;
import com.xabe.elasticsearch.util.Constant;
import com.xabe.elasticsearch.util.JsonUtils;

public class PersonaDAOImpl implements PersonaDAO{

	private ClientProvider provider =   ClientProvider.getInstance();
	
	@Override
	public IndexResponse create(Persona persona) throws IOException {
		return provider.getClient().prepareIndex(Constant.INDEX_PERSONA, Constant.TYPE_PERSONA).setSource(JsonUtils.getJson(persona)).execute().actionGet();
	}
	
	@Override
	public DeleteResponse delete(String id) {
		return provider.getClient().prepareDelete(Constant.INDEX_PERSONA, Constant.TYPE_PERSONA, id).execute().actionGet();	
	}
	
	@Override
	public void update(String id, Persona persona) throws Exception {
		BulkRequestBuilder bulkRequest = provider.getClient().prepareBulk();
		bulkRequest.add(provider.getClient().prepareIndex(Constant.INDEX_PERSONA, Constant.TYPE_PERSONA, id).setSource(JsonUtils.getJson(persona)));
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) 
		{
			throw new Exception(bulkResponse.buildFailureMessage());
		}
	}
	
	@Override
	public Persona search(String id) throws IOException  {
		GetResponse response = provider.getClient().prepareGet(Constant.INDEX_PERSONA, Constant.TYPE_PERSONA, id).execute().actionGet();
		return JsonUtils.getPojo(response.getSourceAsString(), Persona.class);
	}
	
	
	public List<Persona> searchField(String filter,String value) throws IOException {
		SearchResponse response = provider.getClient().prepareSearch(Constant.INDEX_PERSONA)
		        										.setTypes(Constant.TYPE_PERSONA)
		        										.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        										.setQuery(QueryBuilders.queryString(value))          	   
		        										.execute()
		        										.actionGet();
		List<Persona> result = new ArrayList<Persona>();
		for(SearchHit hit : response.getHits()) {
			result.add(JsonUtils.getPojo(hit.getSourceAsString(), Persona.class));
		}
		
		return result;
	}
}
