package org.jetlinks.community.zota.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.zota.dmf.DmfMessageHandler;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ configuration for consuming HawkBit DMF messages.
 * Enabled when zota.dmf.enabled=true (default: true).
 * Compatible with existing JetLinks infrastructure — no RabbitMQ conflict.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "zota.dmf", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DmfRabbitConfig {

    private final ZotaProperties properties;

    @Bean
    public Queue dmfQueue() {
        return new Queue(properties.getDmfQueue(), true);
    }

    @Bean
    public TopicExchange dmfExchange() {
        return new TopicExchange(properties.getDmfExchange());
    }

    @Bean
    public Binding dmfBinding(Queue dmfQueue, TopicExchange dmfExchange) {
        return BindingBuilder.bind(dmfQueue)
                .to(dmfExchange)
                .with(properties.getDmfRoutingKey());
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public SimpleMessageListenerContainer dmfListenerContainer(
            ConnectionFactory connectionFactory,
            MessageListenerAdapter listenerAdapter) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(properties.getDmfQueue());
        container.setMessageListener(listenerAdapter);
        container.setConcurrentConsumers(2);
        container.setMaxConcurrentConsumers(5);
        log.info("DMF RabbitMQ listener configured: queue={}, exchange={}",
                properties.getDmfQueue(), properties.getDmfExchange());
        return container;
    }

    @Bean
    public MessageListenerAdapter listenerAdapter(DmfMessageHandler handler) {
        MessageListenerAdapter adapter = new MessageListenerAdapter(handler, "handleDmfMessage");
        adapter.setMessageConverter(jsonMessageConverter());
        return adapter;
    }
}
