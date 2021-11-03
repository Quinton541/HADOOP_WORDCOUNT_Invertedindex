## 思路

这次只需要执行一个词频统计和一个排序输出的job。和上一次作业分文件词频统计一样，我们在Mapper中采用<word-file,1>的方式输出即可，Reducer分为两部分 先通过Combiner统计出每一个文件中单词的数量，然后分单词统计每个文件中出现的数量,通过List.sort函数将wordlist排序后写入输出文件。

## 运行截图

![image-20211103191820349](/Users/quinton_541/Library/Application Support/typora-user-images/image-20211103191820349.png)

![image-20211103191834917](/Users/quinton_541/Library/Application Support/typora-user-images/image-20211103191834917.png)

输出文件：

![image-20211103191850327](/Users/quinton_541/Library/Application Support/typora-user-images/image-20211103191850327.png)

## 出现的问题

由于作业5已经做过 所以问题比较少 主要是这个问题折磨了很久

在执行程序时，一直提示 ”Could not contain block:...“

网上的教程非常不靠谱，全是提示我datanode出了问题 于是重启hadoop无数次也没解决

![image-20211103183202571](/Users/quinton_541/Library/Application Support/typora-user-images/image-20211103183202571.png)

然后在外网找到一个./hdfs fsck的指令，可以查看hdfs文件系统里面已经损毁的文件

一看就是两个停词的文件被损坏了

![image-20211103183509614](/Users/quinton_541/Library/Application Support/typora-user-images/image-20211103183509614.png)

重新上传之后 问题解决

（后来运行的时候发现有一个输入文件也损坏了 不知道为什么本机的hdfs文件系统为什么这么不靠谱