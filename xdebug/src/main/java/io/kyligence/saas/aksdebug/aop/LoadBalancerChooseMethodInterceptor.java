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

import io.kyligence.saas.aksdebug.Constant;
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
        } catch (DebugInstanceNotFoundException nfe) {
            throw nfe;
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

        final List<String> debugHeaderValues = getDebugHeaderValues(request.getContext());

        final ServiceInstanceListSupplier sils =
                clientFactory.getInstance(name, ServiceInstanceListSupplier.class);
        if (sils == null) {
            return fallback(invocation, "无法正常获取服务列表，回退处理");
        }

        List<ServiceInstance> allInstances = sils.get(request).next().toFuture().get();
        List<ServiceInstance> selectedInstances = selectInstance(name, debugHeaderValues, allInstances);

        if (selectedInstances == null || selectedInstances.isEmpty()) {
            if (debugHeaderValues.isEmpty()) {
                return fallback(invocation, "正常请求没有匹配的实例，回退处理");
            }
            // Debug 请求重新寻找正常 Instance
            selectedInstances = selectInstance(name, Collections.emptyList(), allInstances);
        }

        log.info("匹配 debug 实例 {}", selectedInstances);
        final int randomIndex = ThreadLocalRandom.current().nextInt(selectedInstances.size());
        return Mono.just(new DefaultResponse(selectedInstances.get(randomIndex)));
    }

    private List<String> getDebugHeaderValues(Object reqCtx) {
        if (reqCtx == null) {
            return Collections.emptyList();
        }

        if ((reqCtx instanceof RequestDataContext)
                && ((RequestDataContext) reqCtx).getClientRequest() != null) {
            HttpHeaders headers = ((RequestDataContext) reqCtx).getClientRequest().getHeaders();
            if (headers != null) {
                return headers.getValuesAsList(Constant.REQUEST_HEADER_XDEBUG_KEY);
            }
        }

        return Collections.emptyList();
    }

    private List<ServiceInstance> selectInstance(
            String serviceId, List<String> debugHeaderValues, List<ServiceInstance> serviceInstances) {
        if (serviceInstances.isEmpty()) {
            log.warn("No debug servers available for service: " + serviceId);
            return Collections.emptyList();
        }
        // 正常请求，筛选正常实例，即无 Metadata 标识或者是 SYSTEM 用户启动的
        if (debugHeaderValues.isEmpty()) {
            return serviceInstances.stream()
                .filter(instance -> {
                    if (instance.getMetadata().containsKey(Constant.NACOS_METADATA_XDEBUG_USER_KEY)) {
                        return debugHeaderValues.stream().anyMatch(Constant.SYSTEM_USER_NAME::equalsIgnoreCase);
                    }
                    return true;
                }).collect(Collectors.toList());
        }

        // TODO
        return serviceInstances.stream()
            .filter(instance ->
                debugHeaderValues.stream()
                    .anyMatch(debugHeaderValue ->
                        debugHeaderValue.equals(
                            instance.getMetadata().get(Constant.NACOS_METADATA_XDEBUG_USER_KEY))))
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

    static class DebugInstanceNotFoundException extends RuntimeException {

        DebugInstanceNotFoundException(String errorMsg) {
            super(errorMsg);
        }
    }
}
