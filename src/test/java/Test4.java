import org.apache.flink.util.MathUtils;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/1/12 16:21
 */
public class Test4 {
    public static void main(String[] args) {

        System.out.println(MathUtils.murmurHash(0) % 128);
        System.out.println(MathUtils.murmurHash(1) % 128);

        System.out.println(MathUtils.murmurHash("奇数".hashCode()) % 128);
        System.out.println(MathUtils.murmurHash("偶数".hashCode()) % 128);
    }
}
