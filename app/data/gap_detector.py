"""基于交易时段网格的缺口探测。

不依赖外部行情服务商的日历：用周一至周五 + A 股固定交易时段枚举
"应当存在" 的 K 线时间戳，与实际已有时间戳做差，缺失槽位即缺口。
法定休市日会被误报为缺口，可通过 ignore_gap 标记忽略，不影响主流程。
"""

from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

# A 股各周期的日内收线时刻（左闭区间内已完成的 K 线）
INTRADAY_SLOTS: Dict[str, List[time]] = {
    "60min": [
        time(10, 30), time(11, 30), time(14, 0), time(15, 0),
    ],
    "30min": [
        time(10, 0), time(10, 30), time(11, 0), time(11, 30),
        time(13, 30), time(14, 0), time(14, 30), time(15, 0),
    ],
    "15min": [
        time(9, 45), time(10, 0), time(10, 15), time(10, 30),
        time(10, 45), time(11, 0), time(11, 15), time(11, 30),
        time(13, 15), time(13, 30), time(13, 45), time(14, 0),
        time(14, 15), time(14, 30), time(14, 45), time(15, 0),
    ],
}

# 相邻两根 K 线之间允许的最大时间间隔（不含跨周期），用于无枚举范围时的检测
MAX_GAP_DELTA = {
    "daily": timedelta(days=4),       # 周末 3 天，超过即视为缺口
    "60min": timedelta(hours=2),      # 盘中相邻收线点间隔 1 小时
    "30min": timedelta(hours=1),
    "15min": timedelta(minutes=30),
}


def expected_slots(
    period: str,
    range_start: datetime,
    range_end: datetime,
    holidays: Optional[Sequence[datetime]] = None,
) -> List[datetime]:
    """枚举 [range_start, range_end] 内应当存在的 K 线时间戳。"""
    if range_end < range_start:
        return []

    holiday_dates = {h.date() for h in (holidays or [])}
    slots: List[datetime] = []

    if period == "daily":
        day = range_start.date()
        end_day = range_end.date()
        while day <= end_day:
            if day.weekday() < 5 and day not in holiday_dates:
                slots.append(datetime(day.year, day.month, day.day))
            day += timedelta(days=1)
        return slots

    intraday = INTRADAY_SLOTS.get(period)
    if intraday is None:
        return []

    day = range_start.date()
    end_day = range_end.date()
    while day <= end_day:
        if day.weekday() < 5 and day not in holiday_dates:
            for slot in intraday:
                ts = datetime.combine(day, slot)
                if range_start <= ts <= range_end:
                    slots.append(ts)
        day += timedelta(days=1)
    return slots


def group_consecutive(slots: List[datetime]) -> List[Tuple[datetime, datetime, int]]:
    """把枚举序列中相邻的槽位聚成 (start, end, count) 段。"""
    if not slots:
        return []

    groups: List[Tuple[datetime, datetime, int]] = []
    start = prev = slots[0]
    count = 1

    for ts in slots[1:]:
        if _is_adjacent_slot(prev, ts):
            count += 1
        else:
            groups.append((start, prev, count))
            start = ts
            count = 1
        prev = ts

    groups.append((start, prev, count))
    return groups


def _is_adjacent_slot(a: datetime, b: datetime) -> bool:
    if a.date() == b.date():
        return True  # 同一交易日内的槽位视为连续
    # 跨交易日：a 为当日最后槽位附近、b 为下一工作日槽位即连续
    nxt = a + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return b.date() == nxt.date()


def detect_missing_groups(
    period: str,
    present: Sequence[datetime],
    range_start: datetime,
    range_end: datetime,
    holidays: Optional[Sequence[datetime]] = None,
) -> List[Tuple[datetime, datetime, int]]:
    """缺口检测主入口：返回缺失段列表 (start, end, missing_count)。"""
    present_set = {ts for ts in present if range_start <= ts <= range_end}
    missing = [
        ts for ts in expected_slots(period, range_start, range_end, holidays)
        if ts not in present_set
    ]
    return group_consecutive(missing)


def detect_missing_between_bars(
    period: str,
    present: Sequence[datetime],
    holidays: Optional[Sequence[datetime]] = None,
) -> List[Tuple[datetime, datetime, int]]:
    """无显式请求区间时，只在已有相邻 K 线之间找缺口（不报告边界外缺失）。"""
    ordered = sorted(set(present))
    if len(ordered) < 2:
        return []

    present_set = set(ordered)
    groups: List[Tuple[datetime, datetime, int]] = []
    for prev, nxt in zip(ordered, ordered[1:]):
        missing = [
            ts for ts in expected_slots(period, prev, nxt, holidays)
            if ts not in present_set and ts not in (prev, nxt)
        ]
        groups.extend(group_consecutive(missing))
    return groups
