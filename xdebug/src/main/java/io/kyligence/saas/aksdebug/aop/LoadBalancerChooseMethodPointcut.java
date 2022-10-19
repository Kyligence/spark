package io.kyligence.saas.aksdebug.aop;

import com.alibaba.cloud.nacos.loadbalancer.NacosLoadBalancer;
import org.springframework.aop.ClassFilter;
import org.springframework.aop.MethodMatcher;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.ClassFilters;
import org.springframework.aop.support.StaticMethodMatcher;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.loadbalancer.core.RoundRobinLoadBalancer;

import java.lang.reflect.Method;
import java.util.Arrays;

public class LoadBalancerChooseMethodPointcut implements Pointcut {

    @Override
    public ClassFilter getClassFilter() {
        return ClassFilters.union(
                clazz -> clazz.equals(RoundRobinLoadBalancer.class),
                clazz -> clazz.equals(NacosLoadBalancer.class));
    }

    @Override
    public MethodMatcher getMethodMatcher() {
        return new StaticMethodMatcher() {
            @Override
            public boolean matches(Method method, Class<?> targetClass) {
                return method.getName().equals("choose")
                        && Arrays.equals(method.getParameterTypes(), new Class[]{Request.class});
            }
        };
    }
}
