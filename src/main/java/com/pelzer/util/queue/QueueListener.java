package com.pelzer.util.queue;


public interface QueueListener {
  /**
   * Called by the {@link QueueManager} during each message reception. If any
   * exception is tossed and this listener has been registered as reliable, the
   * message will be pushed back into the head of the queue and reprocessed. 
   */
  void process(String queueName, BaseMessage message) throws QueueException;
}
