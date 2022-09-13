package abc.xyz.index;

import java.util.concurrent.ConcurrentHashMap;

public class IndexManager {
    private final ConcurrentHashMap<String, IndexEntry> index = new ConcurrentHashMap<>(256);

    public void put(String key, IndexEntry indexEntry){
        this.index.put(key, indexEntry);
    }

    public IndexEntry get(String key){
        return this.index.get(key);
    }

    public void remove(String key){
        this.index.remove(key);
    }

    public boolean containsKey(String key){
        return this.index.containsKey(key);
    }

    public int getSize(){
        return this.index.size();
    }
}
