# customTool
## Java Core Library

| Project        | Description                                                  |
| -------------- | ------------------------------------------------------------ |
| cache          | 关于多种缓存场景及缓存框架的实践                             |
| db2es          | 关于数据库同步ES，多线程，大数据量，增量同步数据通用框架     |
| ttlThreadlocal | 关于线程跨线程池传递值，解决异步执行时上下文传递的问题的实践 |
| ttlAgent       | Java agent代理无侵入应用代码的方式实现ttlThreadlocal值传递   |
| wheelTimer     | 关于时间轮算法的实现定时任务框架的实践                       |

## 功能

### cache需求场景

1）本地缓存，针对缓存的数量有限，最近最少使用的缓存进行淘汰的使用场景。

2）本地缓存，基于时间的回收策略，按照缓存的读取，写入时间进行过期数据的使用场景。

3）远程缓存。

4）多级缓存，针对避免缓存击穿和缓存雪崩的使用场景。

5）自动刷新缓存，按时间自动刷新缓存，保证缓存一致性的使用场景

### db2es需求场景

1）对大数据量同步,基于id或自增列进行多线程分批同步，全量同步，增量同步。

### wheelTimer定时任务的使用实践

```
// 构造一个 Timer 实例
Timer timer = new HashedWheelTimer();
 
// 提交一个任务，让它在 5s 后执行
Timeout timeout1 = timer.newTimeout(new TimerTask() {
    @Override
    public void run(Timeout timeout) {
        System.out.println("5s 后执行该任务");
    }
}, 5, TimeUnit.SECONDS);
 
// 再提交一个任务，让它在 10s 后执行
Timeout timeout2 = timer.newTimeout(new TimerTask() {
    @Override
    public void run(Timeout timeout) {
        System.out.println("10s 后执行该任务");
    }
}, 10, TimeUnit.SECONDS);
 
// 取消掉那个 5s 后执行的任务
if (!timeout1.isExpired()) {
    timeout1.cancel();
}
 
// 原来那个 5s 后执行的任务，已经取消了。这里我们反悔了，我们要让这个任务在 3s 后执行
// 我们说过 timeout 持有上、下层的实例，所以下面的 timer 也可以写成 timeout1.timer()
timer.newTimeout(timeout1.task(), 3, TimeUnit.SECONDS);
```

 具体原理见wheelTimer文档

