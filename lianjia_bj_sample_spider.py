#!/usr/bin/env python3
"""
北京链家二手房成交数据采集（东城区/西城区，2021-01 到 2023-12）

说明：
- 默认仅抓取 test 小样本，避免长时间运行。
- 使用 Playwright 打开页面，尽量模拟正常浏览行为。
- 当检测到登录弹窗或验证码页面时，会 sleep 60 秒后继续。
- 经纬度通过高德地图 API 进行地理编码。
"""

from __future__ import annotations

import argparse
import csv
import random
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

AMAP_KEY = "6bd758e9b829f5e86d381eab136d0837"
DEFAULT_OUTPUT_DIR = r"D:\Pythonprojections\house_20260313"
REGIONS = ["dongcheng", "xicheng"]
BASE_URL = "https://bj.lianjia.com/chengjiao"


@dataclass
class HouseDeal:
    administrative_code: str
    province: str
    city: str
    deal_year: str
    region: str
    business_district: str
    community: str
    listing_time: str
    deal_date: str
    deal_price_wan: str
    listing_price_wan: str
    deal_cycle_days: str
    price_adjust_count: str
    showing_count: str
    follow_count: str
    view_count: str
    house_layout: str
    floor: str
    building_area_sqm: str
    layout_structure: str
    inner_area_sqm: str
    building_type: str
    orientation: str
    built_year: str
    decoration: str
    building_structure: str
    heating: str
    ladder_ratio: str
    property_years: str
    elevator: str
    transaction_ownership: str
    usage: str
    house_years: str
    ownership: str
    longitude: str
    latitude: str
    source_url: str


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def maybe_wait_for_human_check(page) -> None:
    """检测登录框 / 验证码页面。检测到则休眠 60 秒。"""
    page_text = page.content()
    hit_keywords = ["登录", "验证码", "安全验证", "人机验证"]
    login_selectors = [
        ".login-panel",
        ".overlay-login",
        ".verify-code",
        "input[type='password']",
    ]

    found_selector = any(page.locator(sel).count() > 0 for sel in login_selectors)
    found_keyword = any(k in page_text for k in hit_keywords)

    if found_selector or found_keyword:
        print("[WARN] 检测到登录/验证码特征，休眠 60 秒后继续……")
        time.sleep(60)


def parse_total_pages(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    page_box = soup.select_one("div.page-box.house-lst-page-box")
    if not page_box:
        return 1
    data_page = page_box.get("page-data", "")
    m = re.search(r'"totalPage":(\d+)', data_page)
    return int(m.group(1)) if m else 1


def parse_list_page(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []
    for li in soup.select("ul.listContent li"):
        a = li.select_one("div.title a")
        deal_info = clean_text(li.select_one("div.dealDate").get_text(" ")) if li.select_one("div.dealDate") else ""
        total_price = clean_text(li.select_one("div.totalPrice").get_text(" ")) if li.select_one("div.totalPrice") else ""
        items.append(
            {
                "detail_url": a["href"] if a and a.has_attr("href") else "",
                "title": clean_text(a.get_text(" ")) if a else "",
                "deal_date": deal_info,
                "deal_price": total_price,
            }
        )
    return items


def parse_detail(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    kv: Dict[str, str] = {}

    for span in soup.select("div.msg span"):
        label = clean_text(span.get_text(" "))
        val = clean_text(span.find_next_sibling(text=True) or "")
        if label:
            kv[label] = val

    for li in soup.select("div.base li"):
        label = clean_text(li.select_one("span").get_text(" ")) if li.select_one("span") else ""
        value = clean_text(li.get_text(" ").replace(label, "", 1)) if label else ""
        if label:
            kv[label] = value

    for li in soup.select("div.transaction li"):
        label = clean_text(li.select_one("span").get_text(" ")) if li.select_one("span") else ""
        value = clean_text(li.get_text(" ").replace(label, "", 1)) if label else ""
        if label:
            kv[label] = value

    community = ""
    c = soup.select_one("a.info")
    if c:
        community = clean_text(c.get_text(" "))
    kv["小区"] = community

    return kv


def geocode_address(address: str) -> Dict[str, str]:
    if not address:
        return {"lng": "", "lat": ""}
    try:
        resp = requests.get(
            "https://restapi.amap.com/v3/geocode/geo",
            params={"key": AMAP_KEY, "address": address, "city": "北京"},
            timeout=15,
        )
        data = resp.json()
        if data.get("status") == "1" and data.get("count") != "0":
            location = data["geocodes"][0]["location"]
            lng, lat = location.split(",")
            return {"lng": lng, "lat": lat}
    except Exception:
        pass
    return {"lng": "", "lat": ""}


def convert_record(region: str, list_item: Dict[str, str], detail: Dict[str, str], lnglat: Dict[str, str]) -> HouseDeal:
    deal_date = list_item.get("deal_date", "")
    year = deal_date[:4] if re.match(r"\d{4}", deal_date) else ""
    return HouseDeal(
        administrative_code="110000" if region in ("dongcheng", "xicheng") else "",
        province="北京市",
        city="北京市",
        deal_year=year,
        region="东城区" if region == "dongcheng" else "西城区",
        business_district=detail.get("所在区域", ""),
        community=detail.get("小区", ""),
        listing_time=detail.get("挂牌时间", ""),
        deal_date=deal_date,
        deal_price_wan=list_item.get("deal_price", "").replace("万", "").strip(),
        listing_price_wan=detail.get("挂牌价格", "").replace("万", "").strip(),
        deal_cycle_days=detail.get("成交周期", ""),
        price_adjust_count=detail.get("调价", ""),
        showing_count=detail.get("带看", ""),
        follow_count=detail.get("关注", ""),
        view_count=detail.get("浏览", ""),
        house_layout=detail.get("房屋户型", ""),
        floor=detail.get("所在楼层", ""),
        building_area_sqm=detail.get("建筑面积", "").replace("㎡", ""),
        layout_structure=detail.get("户型结构", ""),
        inner_area_sqm=detail.get("套内面积", "").replace("㎡", ""),
        building_type=detail.get("建筑类型", ""),
        orientation=detail.get("房屋朝向", ""),
        built_year=detail.get("建成年代", ""),
        decoration=detail.get("装修情况", ""),
        building_structure=detail.get("建筑结构", ""),
        heating=detail.get("供暖方式", ""),
        ladder_ratio=detail.get("梯户比例", ""),
        property_years=detail.get("产权年限", ""),
        elevator=detail.get("配备电梯", ""),
        transaction_ownership=detail.get("交易权属", ""),
        usage=detail.get("房屋用途", ""),
        house_years=detail.get("房屋年限", ""),
        ownership=detail.get("房权所属", ""),
        longitude=lnglat.get("lng", ""),
        latitude=lnglat.get("lat", ""),
        source_url=list_item.get("detail_url", ""),
    )


def in_date_range(deal_date: str, start: str = "2021.01", end: str = "2023.12") -> bool:
    m = re.match(r"(\d{4})\.(\d{2})", deal_date)
    if not m:
        return False
    dt = datetime(int(m.group(1)), int(m.group(2)), 1)
    start_dt = datetime.strptime(start, "%Y.%m")
    end_dt = datetime.strptime(end, "%Y.%m")
    return start_dt <= dt <= end_dt


def crawl(output_dir: Path, sample_size: int = 5, full_run: bool = False) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / ("lianjia_bj_deals_full.csv" if full_run else "lianjia_bj_deals_test.csv")

    rows: List[HouseDeal] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=120)
        context = browser.new_context(locale="zh-CN", user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ))
        page = context.new_page()

        for region in REGIONS:
            page.goto(f"{BASE_URL}/{region}/", wait_until="domcontentloaded", timeout=60000)
            maybe_wait_for_human_check(page)
            total_pages = parse_total_pages(page.content())
            print(f"[INFO] {region} 总页数约: {total_pages}")

            for pg in range(1, total_pages + 1):
                list_url = f"{BASE_URL}/{region}/pg{pg}/"
                print(f"[INFO] 打开列表页: {list_url}")
                try:
                    page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                except PlaywrightTimeoutError:
                    print("[WARN] 列表页超时，跳过。")
                    continue

                maybe_wait_for_human_check(page)
                items = parse_list_page(page.content())
                if not items:
                    continue

                for item in items:
                    if not item.get("detail_url") or not in_date_range(item.get("deal_date", "")):
                        continue

                    try:
                        page.goto(item["detail_url"], wait_until="domcontentloaded", timeout=60000)
                    except PlaywrightTimeoutError:
                        print("[WARN] 详情页超时，跳过。")
                        continue

                    maybe_wait_for_human_check(page)
                    detail = parse_detail(page.content())
                    geocode_query = f"北京市{detail.get('小区', '')}"
                    lnglat = geocode_address(geocode_query)

                    rows.append(convert_record(region, item, detail, lnglat))
                    print(f"[OK] 采集: {item.get('title', '')} | {item.get('deal_date', '')}")

                    time.sleep(random.uniform(1.0, 2.3))

                    if not full_run and len(rows) >= sample_size:
                        break

                if not full_run and len(rows) >= sample_size:
                    break

            if not full_run and len(rows) >= sample_size:
                break

        context.close()
        browser.close()

    if rows:
        with out_file.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
            writer.writeheader()
            for r in rows:
                writer.writerow(asdict(r))

    print(f"[DONE] 输出文件: {out_file} | 行数: {len(rows)}")
    return out_file


def main() -> None:
    parser = argparse.ArgumentParser(description="北京链家成交数据采集")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="CSV 输出目录")
    parser.add_argument("--sample-size", type=int, default=5, help="test 模式样本条数")
    parser.add_argument("--full-run", action="store_true", help="是否全量运行")
    args = parser.parse_args()

    crawl(Path(args.output_dir), sample_size=args.sample_size, full_run=args.full_run)


if __name__ == "__main__":
    main()
