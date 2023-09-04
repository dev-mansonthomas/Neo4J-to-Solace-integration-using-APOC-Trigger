package com.solace.se.tmanson.neo4j.procedure;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;


import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SolaceSendEventHttp
{
  @Context
  public Log log;

  /*
   * Solace credentials
   */
  private static String username = "neo4j";
  private static String password = "neo4j";

  //base URL :  TOPIC (to post on a topic)
  // AH/Neo4J/trigger is the root of the topic
  private static String baseUrl  = "http://172.18.0.3:9001/TOPIC/AH/Neo4J/trigger/";


  //name of the plugin from neo4J
  // ex from neo4J web console :   CALL solaceHttp.event(['label1', 'label2'], {property1: 'value1', property2: 123})
  @Procedure(value = "solaceHttp.event")
  @Description("Event node details through Solace PubSub+")
  public void event(@Name("isUpdate"  ) boolean isUpdate,
                    @Name("labels"    ) List<String> labels,
                    @Name("properties") Map <String, Object> properties) {

    if(log.isDebugEnabled())
    {
      log.debug( "Labels     : `%s`", labels    );
      log.debug( "Properties : `%s`", properties);
    }

    Map<String, Object> message = new HashMap<>();
    message.put("Labels"    , labels);
    message.put("Properties", properties);



    String  id      = (String) properties.get("ID");
    String  type    = (String) properties.get("TYPE");

    String topicSuffix    = type+"/IoT/"+id+"/"+(isUpdate?"updated":"inserted");
    
    boolean successEvent = sendEvent(message, topicSuffix);

    if(successEvent)
    {
      log.info("Event successfully send to queue with properties & labels", properties, labels);
    }
  }


  public boolean sendEvent(Map<String, Object> message, String topixSuffix) {
    String payload;
    try {
      payload = convertToJsonString(message);

      String uri=baseUrl+topixSuffix;

      CloseableHttpClient httpClient = HttpClients.createDefault();
      HttpPost httpPost = new HttpPost(uri);

      // Set request headers
      httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json"); // Replace with your desired content type

      // Encode the username and password for Basic authentication
      String auth        = username + ":" + password;
      String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
      String authHeader  = "Basic " + encodedAuth;

      httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader);

      // Set request body
      StringEntity requestEntity = new StringEntity(payload);
      httpPost.setEntity(requestEntity);

      // Send the request
      CloseableHttpResponse response = httpClient.execute(httpPost);

      // Handle the response
      int         statusCode      = response.getStatusLine().getStatusCode();
      HttpEntity  responseEntity  = response.getEntity();
      String      responseBody    = EntityUtils.toString(responseEntity);
      System.out.println("Response status code: " + uri);
      System.out.println("Response status code: " + statusCode);
      System.out.println("Response body:        " + responseBody);

      // Close the response and HttpClient
      response  .close();
      httpClient.close();
    }
    catch (Exception e)
    {
      log.error("Error in eventing out"+ e.getMessage(), e);
      return false;
    }
    log.info("Published message : " + payload);
    return true;
  }



  private <T> String convertToJsonString(T model)
  {
    ObjectWriter ow = new ObjectMapper().writer();
    String expressionText = null;
    try
    {
      expressionText = ow.writeValueAsString(model);
    }
    catch (Exception e)
    {
      log.error("Error while performing JSON Conversion : "+ e.getMessage(), e);
    }
    return expressionText;
  }

}