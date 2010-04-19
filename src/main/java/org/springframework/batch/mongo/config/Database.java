package org.springframework.batch.mongo.config;

import java.lang.annotation.RetentionPolicy;

/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 18, 2010
 */
@java.lang.annotation.Documented
@java.lang.annotation.Retention(RetentionPolicy.RUNTIME)
@javax.inject.Qualifier
public @interface Database {
    Purpose value();

    public enum Purpose {
        BATCH, APPLICATION
    }

}
