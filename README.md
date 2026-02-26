# ytb_scraper
A crawler tool for scraping video comments from the YouTube platform.
一个爬取YouTube视频网站视频评论的爬虫工具。

注意：使用需要官方api，官方api免费申请入口：https://console.cloud.google.com/



使用方法：

A：运行执行文件

B：

1.安装 Python

先验证：

python --version

输出python版本号则安装成功，报错来这里 https://www.python.org/ 下载

2.在程序文件下打开终端

3.创建虚拟环境

Windows：

python -m venv venv
venv\Scripts\activate

macOS：

python3 -m venv venv
source venv/bin/activate

4.安装依赖

pip install google-api-python-client pandas openpyxl isodate

5.运行

python ytb_scraper.py
<img width="922" height="902" alt="image" src="https://github.com/user-attachments/assets/c87ba132-e828-4c6a-88f3-49392d26abfd" />
