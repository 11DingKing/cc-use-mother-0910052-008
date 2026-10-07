"""按交易周期计算两根已存在K线之间“按规则应当出现”的分钟线位置。

日线以工作日（周一至周五）为网格；分钟线以 A 股两个交易时段
09:30-11:30、13:00-15:00 为网格，K线时间戳取该根K线的结束时刻：
- 60min: 10:30, 11:30, 14:00, 15:00（每日4根）
- 30min: 10:00, 10:30, 11:00, 11:30, 13:30, 14:00, 14:30, 15:00（每日8根）

法定节假日无法离线获知，检测结果按“应有交易日/交易时段”计算，
研究员可对误报缺口执行 ignore 操作留痕。
"""

from datetime import date, datetime, time, timedelta
from typing import List

# (时段开始, 时段结束, K线长度分钟)
_MORNING = (time(9, 30), time(11, 30))
_AFTERNOON = (time(13, 0), time(15, 0))

_INTRADAY_STEPS = {
    "60min": 60,
    "30min": 30,
    "15min": 15,
}


def _slot_times(step_minutes: int) -> List[time]:
    """生成一个交易日内所有K线的结束时刻。"""
    slots: List[time] = []
    for session_start, session_end in (_MORNING, _AFTERNOON):
        start_minutes = session_start.hour * 60 + session_start.minute
        end_minutes = session_end.hour * 60 + session_end.minute
        cursor = start_minutes + step_minutes
        while cursor <= end_minutes:
            slots.append(time(cursor // 60, cursor % 60))
            cursor += step_minutes
    return slots


_SLOTS = {period: _slot_times(step) for period, step in _INTRADAY_STEPS.items()}


def expected_slot_count(period: str, t1: datetime, t2: datetime) -> int:
    """统计区间 (t1, t2)（开区间）内按周期应当出现的K线数量。"""
    if t1 is None or t2 is None or t2 <= t1:
        return 0

    if period == "daily":
        return _weekdays_between(t1.date(), t2.date())

    step = _INTRADAY_STEPS.get(period)
    if step is None:
        return 0

    slots = _SLOTS[period]
    count = 0
    day = t1.date()
    last_day = t2.date()
    while day <= last_day:
        if day.weekday() < 5:
            for slot_time in slots:
                slot_dt = datetime.combine(day, slot_time)
                if t1 < slot_dt < t2:
                    count += 1
        day += timedelta(days=1)
    return count


def expected_slots(period: str, t1: datetime, t2: datetime) -> List[datetime]:
    """返回区间 (t1, t2) 内缺失K线的理论时间戳列表。"""
    if t1 is None or t2 is None or t2 <= t1:
        return []

    if period == "daily":
        result = []
        day = t1.date() + timedelta(days=1)
        while day < t2.date():
            if day.weekday() < 5:
                result.append(datetime.combine(day, time.min))
            day += timedelta(days=1)
        return result

    slots = _SLOTS.get(period, [])
    result: List[datetime] = []
    day = t1.date()
    while day <= t2.date():
        if day.weekday() < 5:
            for slot_time in slots:
                slot_dt = datetime.combine(day, slot_time)
                if t1 < slot_dt < t2:
                    result.append(slot_dt)
        day += timedelta(days=1)
    return result


def _weekdays_between(d1: date, d2: date) -> int:
    """两个日期之间（开区间）的工作日数量。"""
    if d2 <= d1 + timedelta(days=1):
        return 0
    count = 0
    day = d1 + timedelta(days=1)
    while day < d2:
        if day.weekday() < 5:
            count += 1
        day += timedelta(days=1)
    return count
