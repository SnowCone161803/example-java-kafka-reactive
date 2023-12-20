package com.example.cache.spring;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest(classes = {
    ServiceWithCache.class,
    CacheConfiguration.class
})
public class ServiceWithCacheTest {

    @Autowired
    ServiceWithCache serviceWithCache;

    @Test
    public void testFirstReturnsStuff() throws Exception {
        assertThat(serviceWithCache.first()).isEqualTo(1);
        assertThat(serviceWithCache.first()).isEqualTo(1);
    }


    @Test
    public void testSecondReturnsStuff() throws Exception {
        assertThat(serviceWithCache.first()).isEqualTo(1);
        // WARNING!!! note this is equal to 1
        // this is because the cache key for a function with no arguments is always 0
        assertThat(serviceWithCache.second()).isEqualTo(1);
    }
}
