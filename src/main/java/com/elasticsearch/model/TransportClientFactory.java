package com.elasticsearch.model;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
@Configuration
public class TransportClientFactory {

    @Bean
    public TransportClient transportClient() throws UnknownHostException {
        // 一定要注意,9300为elasticsearch的tcp端口
        InetSocketTransportAddress master = new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300);
        // 集群名称
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").put("client.transport.sniff", true).build();
        TransportClient client = new PreBuiltTransportClient(settings);
        // 添加
        client.addTransportAddresses(master);
        return client;
    }
}
