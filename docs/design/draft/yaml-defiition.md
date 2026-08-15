不着急，我们来看一看都有需要定义哪些yaml schema吧，在这个基础之上再设计具体的功能，也就更方便扩展。
- trigger / source / transform / sink 这是一套
- sequential / concurrent / cascaded / batch 这是一套
- Rescenario的场景接近于上面的sequential和concurrent，但是也需要capture、transform，更重要的是assertion（expect）

同时 sohara 可以通过服务的模式启动 `sohara serve`，或者直接执行 `sohara run`，给定对应的yaml文件来运行。

另外：
- yaml文件是否可以导入其他的yaml文件，这样某些公用的step可以共享。
- 执行过程是否可记录、可恢复、流程可以持久化，这样就能定义长时流程。
- 是否加入条件定义，这样就可以实现分支、循环和复杂业务流。
- 是否允许类似human-in-the-loop或者之类的过程，比如中间需要插入approve之类的操作，这样就可以定义企业级的审批/退回等流程。
