package com.dy.minichat.global.id;

import org.springframework.stereotype.Component;

@Component
public class UserChatIdGenerator extends AbstractIdGenerator {

    public UserChatIdGenerator(Snowflake snowflake) {
        super(snowflake);

    }
}