import java.util.concurrent.Callable;

/**
 *
 * 什么时候匿名内部类的传递可以用lambda表达式式替换:
 *      如果接受的接口只有一个抽象方法(default方法没有限制), 可以替换
 *
 *  1. 如果大括号中只有一行代码, 大括号可以省略
 *  2. 用lambda表达式替换，可以用alt+enter一键替换
 *
 */
public class Test1 {
    public static void main(String[] args) throws Exception {

        /**
         * f1方法里要传一个接口，在调用时传一个匿名内部类(new一个该接口)
         */
        /*f1(new Runnable() {
            @Override
            public void run() {
                System.out.println("run....");

            }
        });*/

        /**lambda表达式
         * 该方法在Runnable接口的Run方法里运行
         */
        /*f1(() -> {
            System.out.println("run.....");

        });*/

        /**
         * 省略{}
         */
        // f1(() -> System.out.println("run....."));


        f2(new Callable<String>() {
            @Override
            public String call() throws Exception {

                return "ok";
            }
        });

        /**
         * 只有一行代码，有return的，也要省略
         */
        f2(() -> "ok");

    }

    public static void f1(Runnable runnable) {
        runnable.run();
    }

    public static void f2(Callable<String> callable) throws Exception {
        String call = callable.call();
        System.out.println(call);
    }
}

