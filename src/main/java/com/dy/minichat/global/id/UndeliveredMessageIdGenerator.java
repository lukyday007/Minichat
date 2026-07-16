package com.dy.minichat.global.id;

import org.springframework.stereotype.Component;

@Component
public class UndeliveredMessageIdGenerator extends AbstractIdGenerator {
    public UndeliveredMessageIdGenerator(Snowflake snowflake) {
        super(snowflake);

    }
}