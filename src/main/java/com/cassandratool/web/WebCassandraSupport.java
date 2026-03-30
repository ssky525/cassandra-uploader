package com.cassandratool.web;

import com.datastax.oss.driver.api.core.CqlSession;

import javax.servlet.http.HttpSession;

/**
 * Хранение {@link CqlSession} в HTTP-сессии браузера.
 */
public final class WebCassandraSupport {

    public static final String ATTR_CQL_SESSION = "cassandratool.cqlSession";

    private WebCassandraSupport() {}

    public static CqlSession get(HttpSession session) {
        Object o = session.getAttribute(ATTR_CQL_SESSION);
        return o instanceof CqlSession ? (CqlSession) o : null;
    }

    public static void set(HttpSession session, CqlSession cql) {
        clear(session);
        session.setAttribute(ATTR_CQL_SESSION, cql);
    }

    public static void clear(HttpSession session) {
        Object o = session.getAttribute(ATTR_CQL_SESSION);
        if (o instanceof CqlSession) {
            try {
                ((CqlSession) o).close();
            } catch (Exception ignored) {
                // ignore
            }
        }
        session.removeAttribute(ATTR_CQL_SESSION);
    }

    public static boolean isConnected(HttpSession session) {
        return get(session) != null;
    }
}
