## TinyKV

### appendForPrefixData 

往key带有特定前缀keyPrefix的所有数据的value的末尾追加一个字节appendByte.

##### main函数

往前缀带有 "table:id1" 的所有数据的 value 的末尾追加一个 "0".

##### 实现说明

- 先找出具有 "table:id1"前缀的region范围，在metas中是连续的多个region.
- region分布在不同的节点，更新是独立的，因此可以并行加速，在这里体现为go routine.
- 为了解决数据分布不均的问题，这里实现了一个简易的数据迁移机制，当一个region数据过多时，迁移一部分到数据较少的region中。这是在InsertData的时候检测和迁移的，因此可能会影响插入响应的延迟，但实际上这种检测和迁移的操作一般放在一个后台的任务来完成（虽然这里没有这么实现），会以最大程度减少对插入响应的影响.





