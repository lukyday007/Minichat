package com.dy.minichat.global.id;

public abstract class AbstractIdGenerator {
    protected final Snowflake snowflake;

    protected AbstractIdGenerator(Snowflake snowflake) {
        this.snowflake = snowflake;
    }

    public long generate(){
        return snowflake.nextId();
    }
}