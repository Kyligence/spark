package io.kyligence.saas.aksdebug;

import feign.RequestInterceptor;
import io.kyligence.saas.aksdebug.aop.LoadBalancerChooseMethodInterceptor;
import io.kyligence.saas.aksdebug.aop.LoadBalancerChooseMethodPointcut;
import io.kyligence.saas.aksdebug.feign.PassThroughRequestHeaders;
import org.springframework.aop.Advisor;
import org.springframework.aop.framework.autoproxy.AbstractAdvisorAutoProxyCreator;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClients;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Collections;
import java.util.List;

@ConditionalOnProperty(name = "xdebug")
@Configuration(proxyBeanMethods = false)
@LoadBalancerClients(
        defaultConfiguration = AksDebugAutoConfiguration.DebugableLoadBalancerConfiguration.class)
public class AksDebugAutoConfiguration {

    @Bean
    public RequestInterceptor passThroughRequestHeaders() {
        return new PassThroughRequestHeaders();
    }

    static class DebugableLoadBalancerConfiguration {

        @Bean
        public AbstractAdvisorAutoProxyCreator defaultAdvisorAutoProxyCreator(
                Environment environment, LoadBalancerClientFactory loadBalancerClientFactory) {
            return new AbstractAdvisorAutoProxyCreator() {
                @Override
                protected List<Advisor> findCandidateAdvisors() {
                    return Collections.singletonList(
                            new DefaultPointcutAdvisor(
                                    new LoadBalancerChooseMethodPointcut(),
                                    new LoadBalancerChooseMethodInterceptor(environment, loadBalancerClientFactory)));
                }
            };
        }
    }
}
