## 用法

1. 生成模板 sqlite 文件， bin 目录下有一个生产好的，或者用下列命令生成。

```
fdbserver -r createtemplatedb
```

2. 开始转储受损 sqlite 文件到模板文件，**注意连个路径顺序绝对不能反过来**。

最后一个参数是从哪个 page 开始转储，最小是 2.

```
bin/sos <storage-xxxxxx.sqlite> template.sqlite 2
```

3. 程序完成之后，template.sqlite 里应该有转储的数据。
