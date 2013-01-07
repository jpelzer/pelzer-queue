package com.pelzer.util.queue;


public interface QueueManager {
  final String BEAN_NAME = "com.pelzer.util.queue.QueueManager";
  
  /**
   * Defaults timeoutSeconds to 86400 (1 day). @see #put(String, BaseMessage,
   * int)
   */
  void put(String queueName, BaseMessage message) throws QueueException;
  
  /** Pushes baseMessage into the given queue with a TTL of timeoutSeconds. */
  void put(String queueName, BaseMessage message, int timeoutSeconds) throws QueueException;
  
  /**
   * Registers the given listener to begin receiving messages from the queue.
   * This command returns immediately, and the implementation probably spawns a
   * thread for each listener registered in this manner. If reliable is set
   * true, the system should catch exceptions tossed by
   * {@link QueueListener#process(String, BaseMessage)} and re-push the message
   * back into the backing queue for reprocessing.
   */
  void listen(String queueName, QueueListener listener, boolean reliable);
  
  /**
   * Interrupts the underlying thread and stop the listener from receiving any
   * more messages. If waitForDeath is true, this method will only return once
   * the listener thread has died.
   */
  void unlisten(QueueListener listener, boolean waitForDeath);
}
