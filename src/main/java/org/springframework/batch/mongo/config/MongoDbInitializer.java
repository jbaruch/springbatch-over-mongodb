/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.mongo.config;

import com.mongodb.DB;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

public class MongoDbInitializer implements InitializingBean {

    private boolean initialize = false;

    private DB db;

    private Log logger = LogFactory.getLog(getClass());

    public void setInitialize(boolean initialize) {
        this.initialize = initialize;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(db);
        if (initialize) {
            try {
                db.dropDatabase();
                logger.debug("Database dropped");
            }
            catch (Exception e) {
                logger.debug("Could not drop database", e);
            }
        }
    }

}