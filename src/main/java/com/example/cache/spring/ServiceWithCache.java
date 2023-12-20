package com.example.cache.spring;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
public class ServiceWithCache {

    @Cacheable("lazy")
    public int first() {
        return 1;
    }

    @Cacheable("lazy")
    public int second() {
        return 2;
    }
}
