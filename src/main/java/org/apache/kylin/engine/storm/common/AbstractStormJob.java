package org.apache.kylin.engine.storm.common;

import org.apache.kylin.common.KylinConfig;

import java.io.IOException;

/**
 * Created by copperfield on 22/01/2017.
 */
public abstract class AbstractStormJob {
    
    public static KylinConfig loadKylinPropsAndMetadata() {
        return KylinConfig.getInstanceFromEnv();   // Think about how to get Kylin properties and Configure
    }
}
