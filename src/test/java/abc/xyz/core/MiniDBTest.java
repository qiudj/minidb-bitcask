package abc.xyz.core;

import abc.xyz.exception.MiniDbException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Data
@Builder
@AllArgsConstructor
class User implements Serializable {
    private String id;
    private String name;
    private Integer age;
    private String address;

    public static User getRandomUser(String id){
        return User.builder()
                .id(id)
                .name(UUID.randomUUID().toString().replaceAll("-", ""))
                .age(new Random().nextInt(100)+1)
                .address("桃花岛." + id)
                .build();
    }
}

public class MiniDBTest {
    private MiniDB db;

    // 使用整数作为key进行测试, 设置测试的最大key
    private static final int MAX_TEST_NUM_KEY = 20 * 10000;

    @Before
    public void setup(){
        try {
            this.db = MiniDB.open("test", "./mini-db");
        } catch (MiniDbException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void close(){
        try {
            this.db.close();
        } catch (MiniDbException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void delete() throws MiniDbException {
        String key = "1000";
        this.db.delete(key);
        this.get();
    }

    @Test
    public void get() throws MiniDbException {
        String key = "M00001";
        User u = (User) db.get(key);
        if (u != null){
            System.out.println("user = " + u + ", db size = " + db.getSize());
        }
    }

    @Test
    public void getAndWaitCompact() throws MiniDbException {
        String key = "1000";
        User u = (User) db.get(key);
        System.out.println("user = " + u + ", db size = " + db.getSize());
        try {
            TimeUnit.SECONDS.sleep(90);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        User u1 = (User) db.get(String.valueOf(1000));
        System.out.println("user = " + u + ", db size = " + db.getSize());

        Assert.assertEquals("1000", u1.getId());
        Assert.assertEquals("桃花岛.1000", u1.getAddress());
    }

    @Test
    public void singlePut() throws MiniDbException {
        String key = "M00001";
        User u = User.getRandomUser(key);
        db.put(key, u);

        System.out.println("user = " + u + ", db size = " + db.getSize());
    }

    @Test
    public void put() throws InterruptedException {
        Thread[] workers = new Thread[8];
        for (int j = 0; j < workers.length; j++){
            final int start = j;
            final int step = workers.length;
            workers[j] = new Thread(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    for (int i = start; i < MAX_TEST_NUM_KEY; i += step ){
                        String id = String.valueOf(i);
                        User user = User.getRandomUser(id);
                        db.put(id, user);
                    }
                }
            });
        }
        long start = System.currentTimeMillis();
        for (Thread worker: workers)  worker.start();
        for (Thread worker: workers)  worker.join();

        long end = System.currentTimeMillis();
        long cost = end - start;
        double rate = Math.round(MAX_TEST_NUM_KEY * 1.0 / cost * 1000);

        System.out.println("Batch task done, cost time: " + cost + " ms => " + rate + " entry/s");
    }

    @Test
    public void getBatchSingleThread() throws MiniDbException {
        Random random = new Random();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 20000; i++){
            String key = String.valueOf(random.nextInt(MAX_TEST_NUM_KEY));
            User u = (User) db.get(key);
            if (u != null){
                Assert.assertEquals("桃花岛." + key, u.getAddress());
            }
            System.out.println(u);
        }

        long end = System.currentTimeMillis();
        long cost = end - start;
        double rate = Math.round(MAX_TEST_NUM_KEY * 1.0 / cost * 1000);
        System.out.println("Read task by one thread done, cost time: " + cost + " ms => read rate： " + rate + " entry/s");
    }

    @Test
    public void getBatchMultiThread() throws InterruptedException {
        long start = System.currentTimeMillis();
        Thread[] workers = new Thread[16];
        final int totalReadCount = MAX_TEST_NUM_KEY * workers.length;

        for (int j = 0; j < workers.length; j++){
            workers[j] = new Thread(new Runnable() {
                final Random random = new Random();
                @SneakyThrows
                @Override
                public void run() {
                    for (int i = 0; i < MAX_TEST_NUM_KEY; i++){
                        String key = String.valueOf(random.nextInt(MAX_TEST_NUM_KEY));
                        User u = (User) db.get(key);
                        if (u != null){
                            Assert.assertEquals("桃花岛." + key, u.getAddress());
                        }
//                        System.out.println(u);
                    }
                }
            });
        }
        for (Thread worker: workers)  worker.start();
        for (Thread worker: workers)  worker.join();

        long end = System.currentTimeMillis(), cost = end - start;
        double rate = Math.round(totalReadCount * 1.0 / cost * 1000);
        System.out.println("Read task by multi thread done, cost time: " + cost + " ms => read rate： " + rate + " entry/s");
    }

}