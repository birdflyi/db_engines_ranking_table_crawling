#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2025/8/1 8:26
# @Author : 'Lou Zehua'
# @File   : selenium_request.py

import os
import sys

if '__file__' not in globals():
    # !pip install ipynbname  # Remove comment symbols to solve the ModuleNotFoundError
    import ipynbname

    nb_path = ipynbname.path()
    __file__ = str(nb_path)
cur_dir = os.path.dirname(__file__)
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:  # 解决ipynb引用上层路径中的模块时的ModuleNotFoundError问题
    sys.path.append(pkg_rootdir)
    print('-- Add root directory "{}" to system path.'.format(pkg_rootdir))


import requests

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


def is_challenge_complete(driver):
    """检测挑战是否完成的综合方法"""
    # 检查挑战容器
    try:
        challenge_container = driver.find_element(By.ID, "challenge-container")
        if challenge_container.is_displayed():
            # 挑战仍在进行
            return False
    except:
        pass

    # 检查挑战脚本是否仍在运行
    try:
        if driver.execute_script('return typeof AwsWafIntegration !== "undefined"'):
            # 挑战仍在进行
            return False
    except:
        pass

    # 检查挑战脚本是否消失
    page_source = driver.page_source.lower()
    if "challenge.js" not in page_source:
        # 挑战可能已完成
        return True

    # 检查页面标题
    title = driver.title.lower()
    if title and "aws waf" not in title and "challenge" not in title:
        # 挑战完成
        return True

    return False


def bypass_aws_waf(url):
    """使用 Selenium 可靠地处理 AWS WAF 挑战并获取页面内容"""
    # 配置 Chrome 选项
    chrome_options = Options()

    # 无头模式配置
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # 模拟真实浏览器指纹
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
    chrome_options.add_argument("--window-size=1920,1080")

    # 禁用自动化检测标志
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')

    # 创建持久化用户数据目录（可选）
    # user_data_dir = os.path.join(os.getcwd(), "chrome_profile")
    # chrome_options.add_argument(f"user-data-dir={user_data_dir}")

    # 创建 WebDriver
    service = Service()  # 自动查找 ChromeDriver
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # 执行 JavaScript 修改 navigator 属性
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # 访问目标 URL
        driver.get(url)
        print(f"初始页面标题: {driver.title}")

        # 等待挑战完成 - 使用显式等待
        try:
            WebDriverWait(driver, 60).until(
                lambda d: is_challenge_complete(d)
            )
            print("AWS WAF 挑战确认完成")
        except Exception as e:
            print(f"等待挑战完成超时或出错: {e}")

        # 获取最终页面内容
        final_html = driver.page_source

        # 验证内容是否有效
        if len(final_html) < 1000:
            print("警告: 获取的内容过短，可能未完全通过挑战")
        elif "challenge.js" in final_html.lower():
            print("警告: 挑战脚本仍在页面中，可能未完全通过挑战")

        return final_html

    except Exception as e:
        print(f"处理过程中出错: {e}")
        # 出错时返回当前页面内容
        return driver.page_source if 'driver' in locals() else None

    finally:
        try:
            driver.quit()
        except:
            pass


def get_page_content(url, resp_encoding="utf-8", **kwargs):
    """智能获取页面内容，自动处理WAF挑战"""
    # 首先尝试普通请求（可能不需要挑战）
    try:
        response = requests.get(url, **kwargs)
        response.encoding = resp_encoding

        # 检查是否是挑战页面
        if response.status_code == 202 or "challenge.js" in response.text.lower():
            print("检测到 AWS WAF 挑战，启动浏览器解决方案...")
            return bypass_aws_waf(url)

        # 检查内容是否有效
        if response.status_code == 200 and len(response.text) > 1000:
            return response.text

        return bypass_aws_waf(url)  # 如果内容无效，使用浏览器方案

    except requests.exceptions.RequestException:
        # 如果请求失败，直接使用浏览器方案
        return bypass_aws_waf(url)
    except ImportError:
        # 如果没有安装 requests，直接使用浏览器方案
        return bypass_aws_waf(url)


# 使用示例
if __name__ == "__main__":
    urls = [
        "https://db-engines.com/en/ranking",
        "https://db-engines.com/en/system/searchxml"
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

    timeout = 15

    for url in urls:
        print(f"\n{'=' * 50}")
        print(f"获取: {url}")

        content = get_page_content(url, headers=headers, timeout=timeout)

        if content:
            print(f"获取成功! 内容长度: {len(content)}")

            # 保存内容供检查
            filename = url.split("/")[-1] or "page"
            with open(f"{filename}.html", "w", encoding="utf-8") as f:
                f.write(content)
            print(f"已保存到: {filename}.html")

            # 提取标题验证
            try:
                from bs4 import BeautifulSoup

                soup = BeautifulSoup(content, 'lxml')
                title = soup.title.string if soup.title else "无标题"
                print(f"页面标题: {title}")
            except ImportError:
                print("未安装 BeautifulSoup，跳过标题检查")
        else:
            print("获取失败")
