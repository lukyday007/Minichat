package com.dy.minichat.global.id;

import org.springframework.stereotype.Component;

@Component
public class ChatIdGenerator extends AbstractIdGenerator {

    public ChatIdGenerator(Snowflake snowflake) {
        super(snowflake);
    }

}