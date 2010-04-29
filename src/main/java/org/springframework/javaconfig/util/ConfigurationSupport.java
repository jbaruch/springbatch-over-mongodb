package org.springframework.javaconfig.util;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.*;
import org.springframework.stereotype.Component;


/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 14, 2010
 */
@Component
public class ConfigurationSupport {

    @Autowired
    private AutowireCapableBeanFactory autowireCapableBeanFactory;

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Return the object created by this FactoryBean instance, first invoking
     * any container callbacks on the instance
     *
     * @param fb FactoryBean instance
     * @return the object created by the configured FactoryBean instance
     */
    @SuppressWarnings({"unchecked"})
    public <T> T getObject(FactoryBean fb) {
        try {

            FactoryBean factoryBean = (FactoryBean) getConfigured(fb);
            return (T) factoryBean.getObject();
        }
        catch (Exception ex) {
            // TODO clean up
            throw new RuntimeException(ex);
        }
    }

    /* Invoke callbacks on the object, as though it was configured in the
      * factory
      * @param o object to configure
      * @return object after callbacks have been called on it
      */

    private Object getConfigured(Object o) {
        if (this.autowireCapableBeanFactory == null) {
            throw new UnsupportedOperationException(
                    "Cannot configure object - not running in an AutowireCapableBeanFactory");
        }

        autowireCapableBeanFactory.initializeBean(o, null);

        // TODO could replace with ApplicationContextAwareProcessor call if that class were public
        if (this.applicationContext != null) {
            if (o instanceof ResourceLoaderAware) {
                ((ResourceLoaderAware) o).setResourceLoader(this.applicationContext);
            }
            if (o instanceof ApplicationEventPublisherAware) {
                ((ApplicationEventPublisherAware) o).setApplicationEventPublisher(this.applicationContext);
            }
            if (o instanceof MessageSourceAware) {
                ((MessageSourceAware) o).setMessageSource(this.applicationContext);
            }
            if (o instanceof ApplicationContextAware) {
                ((ApplicationContextAware) o).setApplicationContext(this.applicationContext);
            }
        }

        return o;
    }


}
