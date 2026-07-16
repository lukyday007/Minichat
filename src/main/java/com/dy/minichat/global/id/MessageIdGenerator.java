package com.dy.minichat.global.id;

import org.springframework.stereotype.Component;

@Component
public class MessageIdGenerator extends AbstractIdGenerator{
    public MessageIdGenerator(Snowflake snowflake) {
        super(snowflake);
    }
}