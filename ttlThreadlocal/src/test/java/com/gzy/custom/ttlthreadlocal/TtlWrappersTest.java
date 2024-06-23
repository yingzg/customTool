package com.gzy.custom.ttlthreadlocal;

import java.util.Date;
import java.util.concurrent.*;

import com.gzy.custom.ttlthreadlocal.ttl.TransmittableThreadLocal;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;



@RunWith(SpringRunner.class)
@SpringBootTest(classes ={TtlThreadlocalApplication.class})
public class TtlWrappersTest {

    private ExecutorService executor;
    private static String PARENT = "parent: " + new Date();
    private static String CHILD = "child: " + new Date();

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(3);
    }


    @Test
    public void test_Crr() throws InterruptedException, ExecutionException {
        TransmittableThreadLocal<String> parentValue = new TransmittableThreadLocal<>();
        parentValue.set(PARENT);
        System.out.println("current thread = " + Thread.currentThread().getName() + ",threadMap=" + parentValue.get());

       /* Object capture = TransmittableThreadLocal.Transmitter.capture();
        System.out.println("capture = " + JSON.toJSONString(capture));*/

        Runnable task1 = new Runnable() {
            @Override
            public void run() {
                System.out.println("before set, current thread = " + Thread.currentThread().getName() + "threadMap=" + parentValue.get());
                parentValue.set(CHILD);

             /*   TransmittableThreadLocal<String> childValue = new TransmittableThreadLocal<>();
                childValue.set(CHILD);*/
                System.out.println("after set, current thread = " + Thread.currentThread().getName() + "threadMap=" + parentValue.get());

            /*    Object backup = TransmittableThreadLocal.Transmitter.replay(capture);
                assertEquals(PARENT, parentValue.get());
                TransmittableThreadLocal.Transmitter.restore(backup);
                assertEquals(CHILD, parentValue.get());*/
                /*   System.out.println("task1.holder = " + TransmittableThreadLocal.holder.get());*/
            }
        };
        Runnable task2 = new Runnable() {
            @Override
            public void run() {
                System.out.println("before set, current thread = " + Thread.currentThread().getName() + "threadMap=" + parentValue.get());
                parentValue.set("child");
               /* TransmittableThreadLocal<String> childValue2 = new TransmittableThreadLocal<>();
                childValue2.set("child");*/
                System.out.println("after set, current thread = " + Thread.currentThread().getName() + "threadMap=" + parentValue.get());
/*
                Object backup = TransmittableThreadLocal.Transmitter.replay(capture);
                assertEquals(PARENT, parentValue.get());
                TransmittableThreadLocal.Transmitter.restore(backup);
                assertEquals(CHILD, parentValue.get());*/

                /*                System.out.println("task2.holder = " + TransmittableThreadLocal.holder.get());*/

            }
        };
        Future future1 = executor.submit(task1);
        Future future2 = executor.submit(task2);
        future1.get();
        future2.get();
        /*        System.out.println("TransmittableThreadLocal.holder = " + TransmittableThreadLocal.holder.get());*/
        Assert.assertEquals(PARENT, parentValue.get());

    }

    @Test
    public void testDemo1() throws InterruptedException {


        // 创建一个 CompletableFuture
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "Hello, World!";
        });

        // 尝试取消任务
        boolean cancelled = future.cancel(true);
        System.out.println("Cancelled: " + cancelled);

        // 等待任务完成
        String result = future.join();
        System.out.println("Result: " + result);
    }





}
