package com.pelzer.util.queue;

import java.io.IOException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.KestrelCommandFactory;
import net.rubyeye.xmemcached.utils.AddrUtil;

public class KestrelClientFactory implements MemcachedClientFactory {
  private static final ThreadLocal<MemcachedClient> cachedClient = new ThreadLocal<MemcachedClient>();
  
  @Override
  public MemcachedClient getClient() throws IOException {
    MemcachedClient client = cachedClient.get();
    if (client == null || client.isShutdown()) {
      final MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(MessagingConstants.KESTREL_SERVERS));
      builder.setCommandFactory(new KestrelCommandFactory());
      // builder.setConnectionPoolSize(2);
      client = builder.build();
      client.setPrimitiveAsString(true);
      client.setEnableHeartBeat(false);
      client.setOpTimeout(5000);
      client.getTranscoder().setCompressionThreshold(Integer.MAX_VALUE);
      cachedClient.set(client);
    }
    return client;
  }
  
}
