package org.apache.kylin.engine.storm.common;

/**
 * Created by copperfield on 22/01/2017.
 */
public interface StreamConstants {
    
    /**
     *  ConFiGuration entry names for Storm jobs
     */
    
    String CFG_CUBE_NAME = "cube.name";
    
    int NORMAL_RECORD_LOG_THRESHOLD = 100000;
    int ERROR_RECORD_LOG_THRESHOLD = 100;
}
