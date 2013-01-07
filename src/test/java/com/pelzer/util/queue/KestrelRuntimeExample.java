package com.pelzer.util.queue;

import com.pelzer.util.Absorb;
import com.pelzer.util.Logging;
import com.pelzer.util.Logging.Logger;
import com.pelzer.util.spring.ClasspathScanningJSONObjectRegistrar;

public class KestrelRuntimeExample {
  private static final Logger log = Logging.getLogger(KestrelRuntimeExample.class);

  public static void main(String[] args) throws QueueException {
    log.debug("Initializing JSON Objects...");
    ClasspathScanningJSONObjectRegistrar.scanPath("com.pelzer.util.queue");

    log.debug("Getting queueManager");
    KestrelQueueManager kestrelQueueManager = new KestrelQueueManager();

    kestrelQueueManager.listen("fooQueue", new LoggingListener(), true);

    log.debug("Putting messages...");
    for (int i = 0; i < 5; i++) {
      ExampleBaseMessage message = new ExampleBaseMessage();
      message.setValue(i + 1);
      kestrelQueueManager.put("fooQueue", message);
      Absorb.sleep(1000);
    }

    Absorb.sleep(5000);
    kestrelQueueManager.shutdown();
    Absorb.sleep(5000);
  }

  private static class LoggingListener implements QueueListener {
    private Logging.Logger log = Logging.getLogger(this);

    @Override
    public void process(String queueName, BaseMessage message) throws QueueException {
      log.debug("Got message of type '{}'", message.getClass().getName());
      if (message instanceof ExampleBaseMessage) {
        log.debug("Value of message = {}", ((ExampleBaseMessage) message).getValue());
      }
    }
  }

  public static class ExampleBaseMessage extends BaseMessage {
    @Override
    protected String getIdentifier() {
      return "em";
    }

    private int value;

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }

}
