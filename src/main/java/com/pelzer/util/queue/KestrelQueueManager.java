package com.pelzer.util.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

import com.google.gson.JsonParseException;
import com.pelzer.util.Absorb;
import com.pelzer.util.KillableThread;
import com.pelzer.util.Logging;
import com.pelzer.util.OverridableFields;
import com.pelzer.util.json.JSONObject;
import com.pelzer.util.json.JSONUtil;

public class KestrelQueueManager implements QueueManager {
  private final KestrelClientFactory               kestrelClientFactory;
  private final Map<QueueListener, ListenerThread> listeners = new HashMap<QueueListener, KestrelQueueManager.ListenerThread>();
  private Logging.Logger                           log       = Logging.getLogger(this);

  public KestrelQueueManager() {
    this(new KestrelClientFactory());
  }

  public KestrelQueueManager(final KestrelClientFactory kestrelClientFactory) {
    this.kestrelClientFactory = kestrelClientFactory;
  }

  public void put(final String queueName, final BaseMessage message) throws QueueException {
    put(queueName, message, 60 * 60 * 24);
  }

  /**
   * Shuts down the internal connection and any listener threads. Blocks until
   * the threads have terminated. You can reuse this instance, but all listeners
   * must be re-registered.
   */
  public void shutdown() {
    log.info("Shutting down queueManager...");
    for (ListenerThread thread : listeners.values()) {
      thread.die = true;
    }
    for (ListenerThread thread : listeners.values()) {
      try {
        log.info("Killing thread '{}'", thread.getName());
        synchronized (thread) {
          thread.interrupt();
          thread.wait();
        }
      } catch (InterruptedException ignored) {
      }
    }
    listeners.clear();
    try {
      kestrelClientFactory.getClient().shutdown();
    } catch (IOException ignored) {
    }
    log.info("queueManager is now shutdown.");
  }

  public void put(final String queueName, final BaseMessage message, final int timeoutSeconds) throws QueueException {
    try {
      kestrelClientFactory.getClient().set(queueName, timeoutSeconds, JSONUtil.toJSON(message));
    } catch (final TimeoutException ex) {
      throw new QueueException("Timed out pushing message to queue.", ex);
    } catch (final InterruptedException ex) {
      throw new QueueException("Interrupted while waiting to push message to queue", ex);
    } catch (final MemcachedException ex) {
      throw new QueueException("MemcachedException while pushing message to queue", ex);
    } catch (final IOException ex) {
      throw new QueueException("IOException while getting client connection", ex);
    }
  }

  public void listen(final String queueName, final QueueListener listener, final boolean reliable) {
    final ListenerThread thread = new ListenerThread(queueName, reliable, listener);
    thread.setDaemon(true);
    thread.start();
    thread.setName("[q:" + queueName + "] " + thread.getName());
    listeners.put(listener, thread);
  }

  public void unlisten(final QueueListener listener, final boolean waitForDeath) {
    final ListenerThread thread = listeners.remove(listener);
    if (thread != null) {
      thread.die = true;
      thread.interrupt();
      if (waitForDeath)
        try {
          thread.join();
        } catch (final InterruptedException ignored) {
          Absorb.ignore(ignored);
        }
    }

  }

  private class ListenerThread extends KillableThread {
    private final String         queueName;
    private final boolean        reliable;
    private final QueueListener  listener;
    private MemcachedClient      client;
    private final Logging.Logger log = Logging.getLogger(this);

    public ListenerThread(final String queueName, final boolean reliable, final QueueListener listener) {
      this.queueName = queueName;
      this.reliable = reliable;
      this.listener = listener;
    }

    private MemcachedClient getClient() {
      if (client != null && !client.isShutdown())
        return client;
      client = null;
      while (client == null && !die)
        try {
          client = kestrelClientFactory.getClient();
        } catch (final IOException ex) {
          log.error("IOException building memcache client. Will retry.", ex);
          Absorb.sleep(TimeUnit.SECONDS, KestrelConstants.FAILED_CONNECT_SLEEP_SECONDS);
        }
      return client;
    }

    private void shutdownClient() {
      if (client == null || client.isShutdown())
        return;
      log.info("Shutting down memcache connection.");
      try {
        client.shutdown();
      } catch (final IOException ignored) {
        Absorb.ignore(ignored);
      }
      client = null;
    }

    @Override
    public void run() {

      boolean success = false;
      while (!die)
        try {
          String key = queueName + "/t=" + KestrelConstants.QUEUE_LISTEN_MILLIS;
          if (reliable) {
            if (success)
              key = key + "/close";
            key = key + "/open";
          }
          success = false;
          log.debug("Waiting for nextMessage: {}", key);
          final Object messageObj = getClient().get(key, KestrelConstants.QUEUE_LISTEN_MILLIS + 500);
          if (messageObj != null && messageObj instanceof String) {
            // Got a message, let's deserialize
            final String messageJSON = messageObj.toString();
            log.debug("Incoming message: {}", messageJSON);
            try {
              final JSONObject parsed = JSONUtil.fromJSON(messageJSON);
              log.debug("Message parsed to type: {}", parsed.getClass().getName());
              if (parsed instanceof BaseMessage) {
                log.debug("Calling listener.process(...)");
                listener.process(queueName, (BaseMessage) parsed);
              } else
                log.error("JSONObject is not of type BaseMessage, ignoring: {}", messageJSON);
            } catch (final JsonParseException ex) {
              log.error("JSON Parsing exception while parsing message, ignoring.", ex);
            }
            success = true;
          }
        } catch (final TimeoutException ignored) {
          // These are really annoying, they happen when a process thread dies
          // in a particular way. Kill and restart the queue...
          Absorb.ignore(ignored);
          shutdownClient();
          getClient();
        } catch (final MemcachedException ex) {
          log.error("MemcachedException during get()", ex);
          Absorb.sleep(TimeUnit.SECONDS, 1);
        } catch (InterruptedException ignored) {
        } catch (final Throwable ex) {
          log.error("Throwable during get()", ex);
          Absorb.sleep(TimeUnit.SECONDS, 1);
        }
      shutdownClient();
    }
  }

  private static class KestrelConstants extends OverridableFields {
    /**
     * How long to wait for a message each cycle. Doesn't really have much
     * effect.
     */
    static long QUEUE_LISTEN_MILLIS          = 30 * 1000;
    static long FAILED_CONNECT_SLEEP_SECONDS = 30;

    static {
      new KestrelConstants().init();
    }
  }

}
