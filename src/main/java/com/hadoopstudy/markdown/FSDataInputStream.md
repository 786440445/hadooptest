#FSDataInputStream对象
1. FileSystem对象的open方法返回的是FSDataInputStream对象，而不是java.io类对象。
1. 这个类是继承java.io.DataInputStream的一个特殊类。
1. 且支持随机访问，通过seek方法可以从流的任意位置读取数据。
1. FSDataInputStream类也实现了PositionedReadable接口，从一个指定偏移量出读取文件的一部分.
1. PositionedReadable接口有read(long position, byte[] buffer, int offset, int length)方法。
1. PositionedReadable接口readFully(long position, byte[] buffer, int offset, int length)方法
1. PositionedReadable接口readFully(long position, byte[] buffer)
