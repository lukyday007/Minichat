package com.dy.minichat.global.id;

import org.springframework.stereotype.Component;

@Component
public class UserIdGenerator extends AbstractIdGenerator{
    public UserIdGenerator(Snowflake snowflake) {
        super(snowflake);
    }
}