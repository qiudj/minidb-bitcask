package abc.xyz.core;

import abc.xyz.exception.MiniDbException;

public  interface Storage {
    Object get(String key) throws MiniDbException;
    boolean put(String key, Object value) throws MiniDbException;
    boolean delete(String key) throws MiniDbException;
    boolean exist(String key);
}
