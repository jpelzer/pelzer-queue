package com.pelzer.util.queue;

import com.pelzer.util.json.JSONObject;

public abstract class BaseMessage extends JSONObject{
  /** Defaults to the classname of the message. Will work without calling {@link com.pelzer.util.json.JSONUtil#register(com.pelzer.util.json.JSONObject)}
   * beforehand. If you override this method to shorten the identifier, you'll need to call register before it will
   * correctly deserialize. */
  @Override
  protected String getIdentifier(){
    return this.getClass().getName();
  }
}
