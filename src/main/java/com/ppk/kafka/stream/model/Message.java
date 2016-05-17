package com.ppk.kafka.stream.model;


import java.util.Map;

import lombok.Data;

@Data
public class Message {

  private String authorizationHeader;
  private Map<String, String> headers;
  private Object content;

}