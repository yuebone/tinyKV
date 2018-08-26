## TinyKV

### appendForPrefixData 

往key带有特定前缀keyPrefix的所有数据的value的末尾追加一个字节appendByte.
main函数中：
往前缀带有 "table:id1" 的所有数据的 value 的末尾追加一个 "0".