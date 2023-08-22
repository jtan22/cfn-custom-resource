package com.bw.function;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

public class Handler {
    public String handleMysqlInitialisation(Object event) {
        System.out.println("Handle request: " + event);
        System.setProperty("software.amazon.awssdk.http.service.impl",
                "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService");

        Map<String, String> eventMap = getEventMap(event);
        if (StringUtils.equals(eventMap.get("RequestType"), "Create")) {
            try {
                handleCreate(eventMap);
                sendResponse(eventMap, "SUCCESS");
            } catch (Exception e) {
                e.printStackTrace();
                sendResponse(eventMap, "FAILED");
            }
        } else {
            sendResponse(eventMap, "SUCCESS");
        }
        return "JobFinished";
    }

    private void handleCreate(Map<String, String> eventMap) throws Exception {
        System.out.println("Handle Create");
        List<String> queries = getQueries(System.getenv("REGION"),
                System.getenv("SQL_SCHEME_PARAM"),
                System.getenv("SQL_DATA_PARAM"));
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://" + System.getenv("DB_ENDPOINT") + ":3306/" + System.getenv("DB_NAME");
        try (Connection connection = DriverManager.getConnection(url, System.getenv("DB_USER"), System.getenv("DB_PASSWORD"));
                Statement statement = connection.createStatement()) {
            for (String query : queries) {
                System.out.println("Executing: " + query);
                statement.execute(query);
            }
        }
    }

    private List<String> getQueries(String regionName, String schemeName, String dataName) {
        System.out.println("Getting quesries from Parameter Store");
        List<String> queries = new ArrayList<>();
        Region region = Region.of(regionName);
        try (SsmClient ssmClient = SsmClient.builder().region(region).httpClientBuilder(UrlConnectionHttpClient.builder()).build()) {
            queries.addAll(getQueries(ssmClient, schemeName));
            queries.addAll(getQueries(ssmClient, dataName));
        }
        return queries;
    }

    private List<String> getQueries(SsmClient ssmClient, String parameterName) {
        GetParameterRequest request = GetParameterRequest.builder().name(parameterName).build();
        GetParameterResponse response = ssmClient.getParameter(request);
        return Arrays.asList(StringUtils.split(response.parameter().value(), "\n"));
    }

    public void sendResponse(Map<String, String> eventMap, String status) {
        System.out.println("Send Response status: " + status);
        System.out.println("Send Response event: " + eventMap);

        String responseJson = "{\"Status\":\"" + status
                + "\",\"Reason\":\"" + "Java based Custom Resource"
                + "\",\"PhysicalResourceId\":\"" + "CustomResourcePhysicalID"
                + "\",\"StackId\":\"" + eventMap.get("StackId")
                + "\",\"RequestId\":\"" + eventMap.get("RequestId")
                + "\",\"LogicalResourceId\":\"" + eventMap.get("LogicalResourceId")
                + "\",\"NoEcho\":false,\"Data\":{\"Key\":\"Value\"}}";
        System.out.println("Response JSON: " + responseJson);

        var request = HttpRequest.newBuilder()
                .uri(URI.create(eventMap.get("ResponseURL")))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(responseJson))
                .build();
        var client = HttpClient.newHttpClient();
        System.out.println("Sending Response to stack");
        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Response code: " + response.statusCode());
            System.out.println("Response body: " + response.body());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Finish sending response");
    }

    private Map<String, String> getEventMap(Object event) {
        Gson gson = new Gson();
        return new HashMap<String, String>(gson.fromJson(gson.toJson(event), Map.class));
    }

    /**
     * Manual run to push the CloudFormation Resource out of waiting state.
     *
     * @param args
     */
    public static void main(String[] args) {
        Map<String, String> event = new HashMap<>();
        event.put("ResponseURL", "");
        event.put("StackId", "");
        event.put("RequestId", "");
        event.put("LogicalResourceId", "");
        try {
            new Handler().sendResponse(event, "SUCCESS");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
