package com.pelzer.util.queue;

import java.io.IOException;
import net.rubyeye.xmemcached.MemcachedClient;

public interface MemcachedClientFactory {
  final String BEAN_NAME = "com.pelzer.util.queue.MemcachedClientFactory";
  
  MemcachedClient getClient() throws IOException;
}
