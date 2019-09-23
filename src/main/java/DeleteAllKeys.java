import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class DeleteAllKeys {
    public static void main(String[] args) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMaxTotal(20);

        JedisPool jedisPool = new JedisPool(jedisPoolConfig,"hadoop01",6379);

        Jedis resource = jedisPool.getResource();

        String del = resource.flushDB();

        System.out.println(del);

        resource.close();

    }
}
