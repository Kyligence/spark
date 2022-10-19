package io.kyligence.saas.aksdebug.aop;

import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Slf4j
public class LoadBalancerChooseMethodInterceptor implements MethodInterceptor {

    private final Environment environment;

    private final LoadBalancerClientFactory clientFactory;

    public LoadBalancerChooseMethodInterceptor(
            Environment environment, LoadBalancerClientFactory clientFactory) {
        this.environment = environment;
        this.clientFactory = clientFactory;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        try {
            return invoke0(invocation);
        } catch (Throwable throwable) {
            return failover(invocation, throwable);
        }
    }

    private Object invoke0(MethodInvocation invocation) throws Throwable {
        final String name = environment.getProperty(LoadBalancerClientFactory.PROPERTY_NAME);
        if (!StringUtils.hasText(name)) {
            return fallback(invocation, "无效的服务名，不处理");
        }

        final Object[] args = invocation.getArguments();
        Assert.state(args.length == 1 && Request.class.isAssignableFrom(args[0].getClass()), "?");

        final Request<?> request = (Request<?>) args[0];
        if (!(request instanceof DefaultRequest)) {
            return fallback(invocation, "无效的 Request 类型，不处理");
        }

        final List<String> debugHeaders = getDebugHeader(request.getContext());
        if (debugHeaders.isEmpty()) {
            return fallback(invocation, "非 x-debug 请求，不处理");
        }

        final ServiceInstanceListSupplier sils =
                clientFactory.getInstance(name, ServiceInstanceListSupplier.class);
        if (sils == null) {
            return fallback(invocation, "无法正常获取服务列表，回退处理");
        }

        final List<ServiceInstance> debugInstances =
                sils.get(request)
                        .next()
                        .map(serviceInstances -> selectDebugInstance(name, debugHeaders, serviceInstances))
                        .toFuture().get();
        if (debugInstances == null || debugInstances.isEmpty()) {
            return fallback(invocation, "没有匹配的 debug 实例，回退处理");
        }

        log.info("匹配 debug 实例 {}", debugInstances);
        final int randomIndex = ThreadLocalRandom.current().nextInt(debugInstances.size());
        return Mono.just(new DefaultResponse(debugInstances.get(randomIndex)));
    }

    private List<String> getDebugHeader(Object reqCtx) {
        if (reqCtx == null) {
            return Collections.emptyList();
        }

        if ((reqCtx instanceof RequestDataContext)
                && ((RequestDataContext) reqCtx).getClientRequest() != null) {
            HttpHeaders headers = ((RequestDataContext) reqCtx).getClientRequest().getHeaders();
            if (headers != null) {
                return headers.getValuesAsList("x-debug");
            }
        }

        return Collections.emptyList();
    }

    private List<ServiceInstance> selectDebugInstance(
            String serviceId, List<String> debugHeaders, List<ServiceInstance> serviceInstances) {
        if (serviceInstances.isEmpty()) {
            log.warn("No debug servers available for service: " + serviceId);
            return Collections.emptyList();
        }

        // TODO
        return serviceInstances.stream()
                .filter(
                        serviceInstance ->
                                debugHeaders.stream()
                                        .anyMatch(
                                                debugHeader -> serviceInstance.getMetadata().containsKey(debugHeader)))
                .collect(Collectors.toList());
    }

    private Object fallback(MethodInvocation invocation, String msg) throws Throwable {
        log.warn(msg);
        return invocation.proceed();
    }

    private Object failover(MethodInvocation invocation, Throwable throwable) throws Throwable {
        log.error("x-debug 失效转移", throwable);
        return invocation.proceed();
    }
}
