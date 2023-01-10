package org.apache.kylin.engine.storm;

/**
 * Created by copperfield on 22/01/2017.
 */
public interface IStormInput {
    
    public interface IJsonInputFormat {
        
        public String[] parseJsonInput(Object jsonInput);
    }
}
