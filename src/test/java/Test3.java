/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/1/11 10:45
 */
public class Test3 {
    public static void main(String[] args) {
        f(user -> user.getId());
       // f(User::getId); // 方法引用
        // User::new
    }
    
    public static void f(My1 my1) {
        int test = my1.test(new User(10));
        System.out.println(test);
    }
}

interface My1 {
    int test(User user);
}

class User {
    private int id;
    
    public User(int id) {
        this.id = id;
    }
    
    public int getId() {
        return id;
    }
    
    public void setId(int id) {
        this.id = id;
    }
}