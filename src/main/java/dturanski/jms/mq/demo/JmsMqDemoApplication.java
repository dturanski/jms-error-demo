package dturanski.jms.mq.demo;

import javax.jms.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.annotation.JmsListenerConfigurer;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerEndpointRegistrar;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.stereotype.Component;

@SpringBootApplication
@EnableJms
public class JmsMqDemoApplication implements JmsListenerConfigurer {

    private static Logger logger = LoggerFactory.getLogger(JmsMqDemoApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(JmsMqDemoApplication.class, args);
    }


    @Autowired
    private JmsListenerContainerFactory containerFactory;


    @Override
    public void configureJmsListeners(JmsListenerEndpointRegistrar registrar) {
        registrar.setContainerFactory(containerFactory);
    }

    @Bean
    public JmsListenerContainerFactory<?> containerFactory(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setErrorHandler(t-> logger.error(t.getMessage(), t) );
        factory.setMessageConverter(messageConverter);
        return factory;
    }

    @Bean // Serialize message content to json using TextMessage
    public MessageConverter jacksonJmsMessageConverter() {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");
        return converter;
    }

    @Bean
    ApplicationRunner runner(JmsTemplate jmsTemplate, JmsListenerEndpointRegistry registry) {
        return args -> {
            System.out.println("Sending an email message.");
            jmsTemplate.convertAndSend("DEV.QUEUE.1", new Email("info@example.com", "Hello"));
            jmsTemplate.convertAndSend("DEV.QUEUE.1", new Email("dturanski@example.com", "Hello"));
        };
    }

    @Component
    public class Receiver {
        @JmsListener(destination = "DEV.QUEUE.1", id = "receiver",containerFactory = "containerFactory")
        public void receiveMessage(Email email) {
            System.out.println("Received <" + email + ">");
            if (email.getTo().equals("info@example.com")) {
               throw new RuntimeException("bad recipient");
            }
        }
    }
}
