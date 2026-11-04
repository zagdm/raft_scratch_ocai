package com.myraft;

import com.myraft.config.ClusterProperties;
import com.myraft.config.RaftProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableConfigurationProperties({RaftProperties.class, ClusterProperties.class})
@SpringBootApplication
public class MyRaftApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyRaftApplication.class, args);
    }
}
