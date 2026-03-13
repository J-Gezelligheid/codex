## 北京链家成交数据采集脚本（东/西城区，2021.01-2023.12）

脚本文件：`lianjia_bj_sample_spider.py`

### 功能
- 采集北京链家成交房源（东城区、西城区）在 `2021.01` 到 `2023.12` 的数据。
- 字段覆盖：行政区划代码、城市、成交价格、挂牌价格、成交周期、房屋属性、经纬度等。
- 默认先跑 `test` 小样本（可配置条数），避免直接长时间全量采集。
- 使用高德地图 API 进行经纬度解析（已内置你提供的 key）。
- 包含检测逻辑：检测到登录框/验证码特征时，自动 `time.sleep(60)` 后继续。

### 安装
```bash
pip install -r requirements.txt
python -m playwright install chromium
```

### 先跑 test 小样本（推荐）
```bash
python lianjia_bj_sample_spider.py --sample-size 5 --output-dir "D:\\Pythonprojections\\house_20260313"
```

### 再跑全量
```bash
python lianjia_bj_sample_spider.py --full-run --output-dir "D:\\Pythonprojections\\house_20260313"
```

> Linux 环境下若需要演示，可改成当前目录，例如：`--output-dir ./house_20260313`
