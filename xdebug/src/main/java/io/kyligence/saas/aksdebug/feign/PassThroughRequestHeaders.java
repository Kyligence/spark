package io.kyligence.saas.aksdebug.feign;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import lombok.val;

import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;

import io.kyligence.saas.aksdebug.Constant;
import java.util.Collections;
import java.util.Enumeration;

public class PassThroughRequestHeaders implements RequestInterceptor {

    @Override
    public void apply(RequestTemplate template) {
        val currentRequest = currentRequest();
        if (currentRequest == null) {
            return;
        }

        for (final Enumeration<String> e = currentRequest.getHeaderNames(); e.hasMoreElements(); ) {
            val name = e.nextElement();
            if (Constant.REQUEST_HEADER_XDEBUG_KEY.equals(name)) {
                template.header(name, Collections.list(currentRequest.getHeaders(name)));
            }
        }
    }

    private HttpServletRequest currentRequest() {
        RequestAttributes requestAttr;
        try {
            requestAttr = RequestContextHolder.currentRequestAttributes();
            if (!(requestAttr instanceof ServletRequestAttributes)) {
                return null;
            }
            return ((ServletRequestAttributes) requestAttr).getRequest();
        } catch (Exception e) {
            return null;
        }
    }
}
