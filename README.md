# customTool
## Java Core Library

| Project        | Description                                                  |
| -------------- | ------------------------------------------------------------ |
| cache          | 关于多种缓存场景及缓存框架的实践                             |
| db2es          | 关于数据库同步ES，多线程，大数据量，增量同步数据通用框架     |
| ttlThreadlocal | 关于线程跨线程池传递值，解决异步执行时上下文传递的问题的实践 |
| ttlAgent       | Java agent代理无侵入应用代码的方式实现ttlThreadlocal值传递   |
| wheelTimer     | 关于时间轮算法的实现定时任务框架的实践                       |

## cache功能

### cache缓存类型

1）本地缓存，针对缓存的数量有限，最近最少使用的缓存进行淘汰的使用场景。

2）本地缓存，基于时间的回收策略，按照缓存的读取，写入时间进行过期数据的使用场景。

3）远程缓存。

4）多级缓存，针对避免缓存击穿和缓存雪崩的使用场景。

5）自动刷新缓存，按时间自动刷新缓存，保证缓存一致性的使用场景

### 缓存需求场景

1. 数据最终一致性场景。允许允许短期的缓存与DB数据不一致的情况。

   实现方式：

   1）懒加载实现：只在使用的时候，如果数据未缓存则主动加载缓存数据。对于写场景：更新数据只更新DB，不更新缓存，

   ​	 针对读场景：在缓存未能命中或者过期的情况下，再主动加载缓存。

   懒加载的好处是，不会在数据频繁更新的情况下，更新缓存，只有在需要读缓存的时候才会主动更新，减少系统的写入压力（如果需要持久化缓存的话）。缺点是过期策略是依靠时间过期，因此如果过期时间设置的不合理，会造成过长时间的缓存和DB数据不一致的情况。

   2）自动刷新缓存实现：通过ScheduledExecutorService定时任务框架，定期执行刷新任务，自动更新缓存，可尽量保证缓存和DB数据的一致性。好处是主动更新缓存，可避免一些复杂的缓存的更新时间过长，从而影响缓存的读取。缺点是依赖线程池进行任务刷新，线程池受限于最大线程数和分布式场景的刷新问题。

   3）基于时间的回收策略的缓存实现：按照缓存的读取，写入时间进行主动过期数据。

2. 高频写场景。写场景：更新只更新cache，定时异步更新DB。读场景：读cache，miss后，再查询DB并回写。此场景着重于高频写，不保证数据的高一致性，例如一些写在前端的配置数据或者展示列表数据，高频写场景。（**cache模块未实现此场景**）

3. 数据高一致性场景。（**cache模块未实现此场景**）

4. 针对缓存的数量有限，不需要按时间进行过期，按照最近最少使用的原则进行淘汰的场景。基于LRU缓存实现淘汰策略的缓存

5. 多级缓存，对于优先访问缓存，减少远程缓存访问压力及耗时，避免缓存击穿和缓存雪崩的使用场景。

   

## db2es功能

1. 提供通用能力去全量同步db数据到ES，针对大数据量的同步做了同步速度的优化。
2. 提供通用能力去增量同步db数据到ES。
3. 提供校验es和db的数据差异的能力，并保证数据一致。
4. 可以做到对功能使用方面没有感知的切换索引方案。

### db2es同步基本流程

![](D:\个人项目\usefulTool\wheelTimer\doc\es同步流程图.png)

https://github.com/yingzg/customTool/blob/master/wheelTimer/doc/es%E5%90%8C%E6%AD%A5%E6%B5%81%E7%A8%8B%E5%9B%BE.png



## wheelTimer定时任务的使用实践

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

https://github.com/yingzg/customTool/tree/master/wheelTimer/doc

