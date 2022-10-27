package io.kyligence.saas.aksdebug.nacos;

import io.kyligence.saas.aksdebug.Constant;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "xdebug", havingValue = "true")
public class NacosDiscoveryConfig {

    private final String currentUser = Optional
        .ofNullable(System.getenv(Constant.SYSTEM_ENV_CURRENT_USER_KEY))
        .orElse(Constant.DEFAULT_CURRENT_USER);

    @Bean
    public NacosDiscoveryProperties nacosProperties() {
        NacosDiscoveryProperties nacosDiscoveryProperties = new NacosDiscoveryProperties();
        Map<String, String> metadata = nacosDiscoveryProperties.getMetadata();
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        metadata.put(Constant.NACOS_METADATA_XDEBUG_USER_KEY, currentUser);
        metadata.put(Constant.NACOS_METADATA_XDEBUG_START_TIME_KEY, currentTime);
        return nacosDiscoveryProperties;
    }

}
