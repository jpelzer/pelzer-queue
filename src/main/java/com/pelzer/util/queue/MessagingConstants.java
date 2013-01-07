package com.pelzer.util.queue;

import com.pelzer.util.OverridableFields;

public class MessagingConstants extends OverridableFields {
  /** In the form: "host:port host2:port" */
  public static String KESTREL_SERVERS = "localhost:22133";

  static {
    new MessagingConstants().init();
  }
}
