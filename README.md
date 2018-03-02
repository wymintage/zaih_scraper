### 这个爬虫是为了向在行分析报告提供数据而编写的

#### 除了爬取数据，爬虫实现的功能有：
+ 通过代理ip的API自动获取代理ip构建ip池，自动添加或清除失效ip以维护ip池的大小
+ 通过数据库结构的设计实现断点续爬和进度展示
+ 可以通过日志来显示时间信息或爬虫内部运行情况，便于调试和修改


#### 使用须知：
+ 此爬虫已达到实用阶段，但在代码框架、解释文本、代码可读性等方面还需要进一步完善
+ 需要在run_spider.py文件中，通过选择相应的spidername来运行特定的爬虫
+ 将zaih_scraper/zaih_scraper/settings_sample.py重命名为settings.py以便使配置文件生效

#### 项目规划：
+ 继续完善爬虫逻辑和架构
+ 实现命令控制和参数借口
+ 实现一键爬取
