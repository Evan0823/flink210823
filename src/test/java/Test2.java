/**
 *
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/1/11 10:28
 */
public class Test2 {
    public static void main(String[] args) throws Exception {
        f1(a -> a + 10 + "");
        
    }
    
    public static void f1(My my) {
        String result = my.test(10);
        System.out.println(result);
    }
}
// 自定义的一个接口(只要一个抽象方法)
interface My {
    String test(int a);
}
/*
 
 
 */