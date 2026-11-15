#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""load.py — 把 lanes/ 下三份互不相通的轨迹源归一化写入 SQLite。

输入（格式各异）：
  * lanes/shunda.csv     顺达快运  CSV       运单号,节点时间,节点代码,城市
  * lanes/diandian.json  点点众包  JSON 数组 [{wb, ts, ev, st}, ...]
  * lanes/ganxian.txt    干线网络  定宽文本   16 运单号 + 15 时间(YYYYMMDD HHMMSS)
                                            + 5 事件码 + 站点名

归一模型：
  events         每条事件一行，统一字段；保留原始码、归一类型、来源行号、
                 同时间戳内的先后序号（由文件行序决定）
  waybills       每单一行：承运商 / 揽收时间 / 最终时间 / 事件数 / 是否经历退回
                 · 揽收时间 = 最早 pickup 事件的时刻（无 pickup 事件则为 NULL）
                 · 最终时间 = 按 (时间, 同刻行序) 排序后最后一条事件的时刻
  rejected_rows  一级坏件：结构上解析不了的行，原文单独存档
  unknown_codes  二级坏件：事件码不在已知集合，按码另立登记
                  （其事件仍保留在 events，code_known=0）
  code_mapping   码表映射，便于审计
  load_stats     各源原始行数与两级坏件计数

约定：
  * 文件行序即事件先后；同一时间戳落两个节点时用 seq_in_ts 区分 0,1,...
  * JSON 的 source_line 是数组元素序号（1 起），CSV/TXT 是物理行号（表头算第 1 行）
  * 干线网络没有“揽收”码（LNOUT 是出库发运，站点均为中转场/分拨中心），
    故其 waybills.pickup_at 为 NULL，不做臆造
  * had_return = 事件中出现归一类型 reject 或 return

用法：
    python3 -I load.py --lanes lanes --db warehouse.db --summary load_summary.txt
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime

CAR_SD = "顺达快运"
CAR_DD = "点点众包"
CAR_GX = "干线网络"

SH_FILE = "shunda.csv"
DD_FILE = "diandian.json"
GX_FILE = "ganxian.txt"

SH_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
DD_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")
GX_RE = re.compile(r"^(.{16})(\d{8} \d{6})(.{5})(.+)$")

# 已知事件码 -> (归一类型, 说明)
# 表外的码即二级坏件，事件照常保留但 event_type 置 NULL。
MAPPINGS = {
    CAR_SD: {
        "S10": ("pickup", "揽收"),
        "S30": ("transit", "正向中转"),
        "S31": ("transit", "正向中转"),
        "S40": ("arrive", "到达派送站"),
        "S50": ("out_for_delivery", "派送"),
        "S52": ("out_for_delivery", "二次派送（退回后重派，按序列推断）"),
        "S60": ("delivered", "签收"),
        "R10": ("reject", "拒收（按序列推断）"),
        "R20": ("return", "退回中转（按序列推断）"),
        "R30": ("return", "退回中转（按序列推断）"),
    },
    CAR_DD: {
        "pickup": ("pickup", "揽收"),
        "arrive": ("arrive", "到达"),
        "deliver": ("delivered", "送达/签收"),
        "reject": ("reject", "拒收"),
        "return": ("return", "退回"),
    },
    CAR_GX: {
        "LNOUT": ("depart", "出库/发运"),
        "LNARR": ("arrive", "到达"),
        "LNTRF": ("transit", "中转"),
        "LNDLV": ("delivered", "送达/签收"),
        "LNRTN": ("return", "退回"),
    },
}

REASON_TEXT = {
    "field_count": "列数不符",
    "missing_fields": "缺少必填字段",
    "bad_timestamp": "时间无法解析",
    "fixed_width": "定宽布局不匹配",
    "shape": "元素结构异常",
}


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig") as fh:
        return fh.read()


# ---------------------------------------------------------------- 时间解析
def parse_sh_time(s: str):
    if not SH_TIME_RE.match(s):
        return None, f"时间 '{s}' 不符合 YYYY-MM-DD HH:MM:SS"
    try:
        datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        return None, f"时间 '{s}' 非法：{exc}"
    return s, None


def parse_dd_time(s: str):
    if not DD_TIME_RE.match(s):
        return None, f"时间 '{s}' 不符合 YYYY-MM-DDTHH:MM:SS"
    try:
        datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except ValueError as exc:
        return None, f"时间 '{s}' 非法：{exc}"
    return s.replace("T", " "), None


def parse_gx_time(date_part: str, time_part: str):
    try:
        parsed = datetime.strptime(date_part + time_part, "%Y%m%d%H%M%S")
    except ValueError as exc:
        return None, f"时间 '{date_part} {time_part}' 非法：{exc}"
    if not 1990 <= parsed.year <= 2100:
        return None, f"时间 '{date_part} {time_part}' 年份 {parsed.year} 越界"
    return (f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]} "
            f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"), None


# ---------------------------------------------------------------- 结构
class Event:
    __slots__ = ("carrier", "waybill_no", "event_time", "event_type", "raw_code",
                 "station", "code_known", "seq_in_ts", "source_file",
                 "source_line", "source_seq")

    def __init__(self):
        for name in self.__slots__:
            setattr(self, name, None)


class Reject:
    __slots__ = ("carrier", "source_file", "source_line", "reason", "detail",
                 "raw_content", "repair_hint")

    def __init__(self):
        for name in self.__slots__:
            setattr(self, name, None)


class Sink:
    """按源收集事件/坏件，并维护同 (运单,时间戳) 的先后序号。"""

    def __init__(self, carrier: str, filename: str):
        self.carrier = carrier
        self.filename = filename
        self.mapping = MAPPINGS[carrier]
        self.events: list[Event] = []
        self.rejects: list[Reject] = []
        self._ts_seq = defaultdict(int)
        self._seq = 0

    def add_event(self, waybill_no, event_time, raw_code, station, source_line):
        ev = Event()
        ev.carrier = self.carrier
        ev.waybill_no = waybill_no
        ev.event_time = event_time
        mapped = self.mapping.get(raw_code)
        ev.event_type = mapped[0] if mapped else None
        ev.raw_code = raw_code
        ev.station = station
        ev.code_known = 1 if mapped else 0
        key = (waybill_no, event_time)
        ev.seq_in_ts = self._ts_seq[key]
        self._ts_seq[key] += 1
        self._seq += 1
        ev.source_seq = self._seq
        ev.source_file = self.filename
        ev.source_line = source_line
        self.events.append(ev)

    def add_reject(self, source_line, reason, detail, raw_content, repair_hint=None):
        rj = Reject()
        rj.carrier = self.carrier
        rj.source_file = self.filename
        rj.source_line = source_line
        rj.reason = reason
        rj.detail = detail
        rj.raw_content = raw_content
        rj.repair_hint = repair_hint
        self.rejects.append(rj)


# ---------------------------------------------------------------- 顺达 CSV
def load_shunda(path: str, sink: Sink) -> int:
    content = read_text(path)
    physical = content.splitlines()
    buf = io.StringIO(content)
    reader = csv.reader(buf)
    try:
        header = next(reader)
    except StopIteration:
        return 0
    if [h.strip() for h in header] != ["运单号", "节点时间", "节点代码", "城市"]:
        print(f"[warn] {SH_FILE} 表头与预期不一致: {header}", file=sys.stderr)

    prev_line = reader.line_num
    units = 0
    for rec in reader:
        start = prev_line + 1
        end = reader.line_num
        prev_line = end
        raw = "\n".join(physical[start - 1:end])
        if not rec or all(not c.strip() for c in rec):
            continue
        units += 1

        if len(rec) != 4:
            sink.add_reject(
                start, "field_count",
                f"应为 4 列，实得 {len(rec)} 列"
                + ("；缺少 城市" if len(rec) == 3 else "；行被截断"),
                raw)
            continue

        wb, ts, code, city = (c.strip() for c in rec)
        if not wb:
            sink.add_reject(start, "missing_fields", "运单号为空", raw)
            continue
        norm_ts, err = parse_sh_time(ts)
        if err:
            sink.add_reject(start, "bad_timestamp", err, raw)
            continue
        if not code:
            sink.add_reject(start, "missing_fields", "节点代码为空", raw)
            continue
        sink.add_event(wb, norm_ts, code, city or None, start)
    return units


# ---------------------------------------------------------------- 点点 JSON
def load_diandian(path: str, sink: Sink) -> int:
    content = read_text(path)
    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{DD_FILE} 不是合法 JSON: {exc}")
    if not isinstance(data, list):
        raise SystemExit(f"{DD_FILE} 顶层应为数组，实得 {type(data).__name__}")

    for idx, elem in enumerate(data, 1):
        raw = json.dumps(elem, ensure_ascii=False, separators=(",", ":"))
        if not isinstance(elem, dict):
            sink.add_reject(idx, "shape", "数组元素不是对象", raw)
            continue
        wb = elem.get("wb")
        ts = elem.get("ts")
        ev = elem.get("ev")
        st = elem.get("st")
        if not wb or not isinstance(wb, str):
            sink.add_reject(idx, "missing_fields", "缺少 wb", raw)
            continue
        if not ev or not isinstance(ev, str):
            sink.add_reject(idx, "missing_fields", "缺少 ev", raw)
            continue
        if not ts or not isinstance(ts, str):
            sink.add_reject(idx, "missing_fields", "缺少 ts", raw)
            continue
        norm_ts, err = parse_dd_time(ts)
        if err:
            sink.add_reject(idx, "bad_timestamp", err, raw)
            continue
        station = st if isinstance(st, str) and st else None
        sink.add_event(wb, norm_ts, ev, station, idx)
    return len(data)


# ---------------------------------------------------------------- 干线定宽
def gx_candidate(line: str, known_wbs: set, known_stations: set):
    """单字增删试探：给出可还原为合法记录的候选，仅作提示，绝不入库。

    评分优先级：
      3 = 删除一个字后运单号与原文前 16 位一致（多插了一列，保留本单）
      2 = 删除/插入一个字后运单号是已知运单（少了一列）
      另需满足 时间年份合法 且 站点名是已知站点。
    """
    scored = []
    for cand in (line[:i] + line[i + 1:] for i in range(len(line))):
        m = GX_RE.match(cand)
        if not m:
            continue
        wb, stamp, code, station = m.groups()
        if code not in MAPPINGS[CAR_GX] or station not in known_stations:
            continue
        if parse_gx_time(stamp[:8], stamp[9:])[0] is None:
            continue
        if wb == line[:16]:
            scored.append((3, cand))
        elif wb in known_wbs:
            scored.append((2, cand))
    for i in range(len(line) + 1):
        for d in "0123456789":
            cand = line[:i] + d + line[i:]
            m = GX_RE.match(cand)
            if not m:
                continue
            wb, stamp, code, station = m.groups()
            if (wb not in known_wbs or code not in MAPPINGS[CAR_GX]
                    or station not in known_stations):
                continue
            if parse_gx_time(stamp[:8], stamp[9:])[0] is None:
                continue
            scored.append((2, cand))
    if not scored:
        return None
    best = max(s for s, _ in scored)
    return sorted(c for s, c in scored if s == best)[0]


def load_ganxian(path: str, sink: Sink) -> int:
    content = read_text(path)
    lines = content.splitlines()

    # 先用能完整匹配的行建立该源合法运单/站点集合（用于错位行修复提示）
    known_wbs = set()
    known_stations = set()
    for line in lines:
        m = GX_RE.match(line)
        if m:
            t, _ = parse_gx_time(m.group(2)[:8], m.group(2)[9:])
            if t:
                known_wbs.add(m.group(1))
                known_stations.add(m.group(4))

    units = 0
    for lineno, line in enumerate(lines, 1):
        if not line.strip():
            continue
        units += 1
        m = GX_RE.match(line)
        good = False
        if m:
            wb, stamp, code, station = m.groups()
            t, err = parse_gx_time(stamp[:8], stamp[9:])
            if t:
                good = True
                sink.add_event(wb, t, code, station.strip(), lineno)
            else:
                sink.add_reject(lineno, "bad_timestamp", err, line)
        if good:
            continue
        if not m:
            if len(line) < 37:
                detail = f"长度 {len(line)}，不足最短 37 字符，判为截断"
                hint = None
            else:
                detail = f"长度 {len(line)}，字段边界错位"
                hint = gx_candidate(line, known_wbs, known_stations)
                if hint:
                    detail += f"；单字增删可还原为 '{hint}'（仅提示，未采用）"
            sink.add_reject(lineno, "fixed_width", detail, line, hint)
    return units


# ---------------------------------------------------------------- 入库
DDL = """
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS waybills;
DROP TABLE IF EXISTS rejected_rows;
DROP TABLE IF EXISTS unknown_codes;
DROP TABLE IF EXISTS code_mapping;
DROP TABLE IF EXISTS load_stats;

CREATE TABLE events (
    event_id     INTEGER PRIMARY KEY,
    carrier      TEXT    NOT NULL,
    waybill_no   TEXT    NOT NULL,
    event_time   TEXT    NOT NULL,
    event_type   TEXT,
    raw_code     TEXT    NOT NULL,
    station      TEXT,
    code_known   INTEGER NOT NULL,
    seq_in_ts    INTEGER NOT NULL,
    source_file  TEXT    NOT NULL,
    source_line  INTEGER NOT NULL,
    source_seq   INTEGER NOT NULL
);
CREATE INDEX idx_events_wb ON events (carrier, waybill_no, event_time, seq_in_ts);

CREATE TABLE waybills (
    carrier      TEXT    NOT NULL,
    waybill_no   TEXT    NOT NULL,
    pickup_at    TEXT,
    final_at     TEXT,
    event_count  INTEGER NOT NULL,
    had_return   INTEGER NOT NULL,
    PRIMARY KEY (carrier, waybill_no)
);

CREATE TABLE rejected_rows (
    reject_id    INTEGER PRIMARY KEY,
    carrier      TEXT    NOT NULL,
    source_file  TEXT    NOT NULL,
    source_line  INTEGER NOT NULL,
    reason       TEXT    NOT NULL,
    detail       TEXT,
    raw_content  TEXT    NOT NULL,
    repair_hint  TEXT
);
CREATE INDEX idx_rejected_src ON rejected_rows (source_file, source_line);

CREATE TABLE unknown_codes (
    carrier      TEXT    NOT NULL,
    raw_code     TEXT    NOT NULL,
    event_count  INTEGER NOT NULL,
    first_source_line INTEGER,
    sample_station    TEXT,
    PRIMARY KEY (carrier, raw_code)
);

CREATE TABLE code_mapping (
    carrier      TEXT    NOT NULL,
    raw_code     TEXT    NOT NULL,
    event_type   TEXT    NOT NULL,
    note         TEXT,
    PRIMARY KEY (carrier, raw_code)
);

CREATE TABLE load_stats (
    source_file  TEXT    PRIMARY KEY,
    carrier      TEXT    NOT NULL,
    unit_label   TEXT    NOT NULL,
    raw_units    INTEGER NOT NULL,
    loaded_events INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    unknown_code_events INTEGER NOT NULL,
    note         TEXT
);
"""


def build_waybills(events: list[Event]):
    groups: dict[tuple[str, str], list[Event]] = defaultdict(list)
    for ev in events:
        groups[(ev.carrier, ev.waybill_no)].append(ev)

    rows = []
    for (carrier, wb), evs in groups.items():
        evs.sort(key=lambda e: (e.event_time, e.seq_in_ts))
        pickups = [e.event_time for e in evs if e.event_type == "pickup"]
        rows.append((
            carrier,
            wb,
            min(pickups) if pickups else None,
            evs[-1].event_time,
            len(evs),
            1 if any(e.event_type in ("reject", "return") for e in evs) else 0,
        ))
    rows.sort(key=lambda r: (r[0], r[1]))
    return rows


def write_db(db_path, sinks: list[Sink], stats_rows):
    events = [ev for sink in sinks for ev in sink.events]
    rejects = [rj for sink in sinks for rj in sink.rejects]
    # 事件按源内先后写入，保证 event_id 与文件顺序一致
    ordered = sorted(events, key=lambda e: (e.carrier, e.source_seq))

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(DDL)

        for carrier, mapping in MAPPINGS.items():
            for code, (etype, note) in mapping.items():
                conn.execute(
                    "INSERT INTO code_mapping VALUES (?,?,?,?)",
                    (carrier, code, etype, note))

        conn.executemany(
            "INSERT INTO events (carrier, waybill_no, event_time, event_type,"
            " raw_code, station, code_known, seq_in_ts, source_file,"
            " source_line, source_seq) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [(e.carrier, e.waybill_no, e.event_time, e.event_type, e.raw_code,
              e.station, e.code_known, e.seq_in_ts, e.source_file,
              e.source_line, e.source_seq) for e in ordered])

        conn.executemany(
            "INSERT INTO waybills VALUES (?,?,?,?,?,?)", build_waybills(ordered))

        conn.executemany(
            "INSERT INTO rejected_rows (carrier, source_file, source_line,"
            " reason, detail, raw_content, repair_hint) VALUES (?,?,?,?,?,?,?)",
            [(r.carrier, r.source_file, r.source_line, r.reason, r.detail,
              r.raw_content, r.repair_hint) for r in rejects])

        # 二级坏件：按 (承运商, 码) 聚合
        agg = {}
        for e in ordered:
            if e.code_known:
                continue
            key = (e.carrier, e.raw_code)
            if key not in agg:
                agg[key] = [0, e.source_line, e.station]
            agg[key][0] += 1
        conn.executemany(
            "INSERT INTO unknown_codes VALUES (?,?,?,?,?)",
            [(c, code, n, line, st) for (c, code), (n, line, st) in agg.items()])

        conn.executemany(
            "INSERT INTO load_stats VALUES (?,?,?,?,?,?,?,?)", stats_rows)

        conn.commit()
    finally:
        conn.close()
    return len(ordered), len(rejects), agg


# ---------------------------------------------------------------- 摘要
def _reject_key(rj):
    """从坏件原文推出 (运单号, 时间前缀, 码) 用于反查是否已有完整行。

    时间只保留能确定的部分；太短的片段不做匹配，避免误判。
    """
    if rj.carrier == CAR_SD:
        parts = [p.strip() for p in rj.raw_content.split(",")]
        if len(parts) < 2 or not parts[0]:
            return None
        wb, ts = parts[0], parts[1].replace("T", " ")
        code = parts[2] if len(parts) >= 3 and parts[2].strip() else None
        digits = "".join(ch for ch in ts if ch.isdigit())
        if len(digits) < 10:
            return None
        return wb, digits, code
    if rj.carrier == CAR_GX:
        if len(rj.raw_content) < 26 or not rj.raw_content[:16]:
            return None
        wb = rj.raw_content[:16]
        digits = "".join(ch for ch in rj.raw_content[16:32] if ch.isdigit())
        if len(digits) < 10:
            return None
        return wb, digits, None
    return None


def dup_note(rejects, events):
    """若坏件与同单已有完整事件重合，在 detail 里标注：丢弃无信息损失。"""
    by_wb: dict[tuple[str, str], list] = defaultdict(list)
    for e in events:
        digits = "".join(ch for ch in e.event_time if ch.isdigit())
        by_wb[(e.carrier, e.waybill_no)].append((digits, e.raw_code))

    for rj in rejects:
        key = _reject_key(rj)
        if not key:
            continue
        wb, digits, code = key
        hits = [c for d, c in by_wb.get((rj.carrier, wb), ())
                if d.startswith(digits) and (code is None or c == code)]
        if len(hits) == 1:  # 唯一命中才下结论；多处命中说明信息不足以判定
            rj.detail = ((rj.detail or "").rstrip("；")
                         + "；同单已有完整行，丢弃无信息损失")


def fmt_table(rows, headers):
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(str(cell)))
    sep = "  "
    lines = [sep.join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    lines.append(sep.join("-" * widths[i] for i in range(len(headers))))
    for r in rows:
        lines.append(sep.join(str(c).ljust(widths[i]) for i, c in enumerate(r)))
    return lines


def make_summary(db_path, lanes_dir, sinks, stats_rows, n_events, n_rejects, agg):
    events = [ev for s in sinks for ev in s.events]
    waybill_rows = build_waybills(events)
    per_carrier_wb = Counter(r[0] for r in waybill_rows)
    returns = [r for r in waybill_rows if r[5] == 1]
    no_pickup = [r for r in waybill_rows if r[2] is None]

    out = []
    out.append("=" * 68)
    out.append(" 轨迹归一化加载摘要")
    out.append("=" * 68)
    out.append(f"数据库      : {os.path.abspath(db_path)}")
    out.append(f"数据目录    : {os.path.abspath(lanes_dir)}")
    out.append(f"生成时间    : {datetime.now():%Y-%m-%d %H:%M:%S}")
    out.append("")

    out.append("[源级计数]")
    rows = []
    for sr in stats_rows:
        rows.append([sr[0], sr[1], sr[2], sr[3], sr[4], sr[5], sr[6]])
    totals = ["合计", "—", "—",
              sum(r[3] for r in rows), sum(r[4] for r in rows),
              sum(r[5] for r in rows), sum(r[6] for r in rows)]
    rows.append(totals)
    out.extend(fmt_table(rows, ["源文件", "承运商", "原始单位", "原始数量",
                                "入库事件", "一级坏件", "二级坏件"]))
    out.append("")
    out.append("  一级坏件 = 结构上解析不了的行 → rejected_rows（原文存档）")
    out.append("  二级坏件 = 结构可解析但事件码不在已知集合 → unknown_codes"
               "（事件仍入库，code_known=0）")
    out.append("")

    out.append("[入库总量]")
    out.append(f"事件 {n_events} 条，运单 {len(waybill_rows)} 单，一级坏件 {n_rejects} 行")
    out.append("运单分布：" + "，".join(
        f"{c} {n} 单" for c, n in sorted(per_carrier_wb.items())))
    out.append(f"经历退回的运单 {len(returns)} 单（"
               + "，".join(f"{c} {sum(1 for r in returns if r[0]==c)}"
                          for c in sorted(per_carrier_wb)) + "）")
    if no_pickup:
        by_c = Counter(r[0] for r in no_pickup)
        out.append(f"揽收时间为 NULL 的运单 {len(no_pickup)} 单（"
                   + "，".join(f"{c} {n}" for c, n in sorted(by_c.items()))
                   + "）：源内无 pickup 类事件，未臆造")
    out.append("")

    out.append("[一级坏件明细]")
    rj_rows = []
    for rj in sorted((r for s in sinks for r in s.rejects),
                     key=lambda r: (r.source_file, r.source_line)):
        rj_rows.append([rj.source_file, rj.source_line,
                        REASON_TEXT.get(rj.reason, rj.reason), rj.detail or ""])
    if rj_rows:
        out.extend(fmt_table(rj_rows, ["源文件", "行/元素", "原因", "说明"]))
    else:
        out.append("（无）")
    out.append("")

    out.append("[二级坏件登记]")
    uc_rows = [[c, code, n, line, st or ""]
               for (c, code), (n, line, st) in sorted(agg.items())]
    if uc_rows:
        out.extend(fmt_table(uc_rows, ["承运商", "事件码", "次数", "首见行", "样例站点"]))
    else:
        out.append("（无）")
    out.append("")

    hints = [r for s in sinks for r in s.rejects if r.repair_hint]
    if hints:
        out.append("[提示]")
        out.append(f"· {len(hints)} 条列位错位行可由单字增删还原，"
                   "仅记录提示（rejected_rows.repair_hint），未入库")
    notes = [sr for sr in stats_rows if sr[7]]
    for sr in notes:
        out.append(f"· {sr[0]}：{sr[7]}")
    out.append("=" * 68)
    return "\n".join(out)


# ---------------------------------------------------------------- main
def main(argv=None):
    ap = argparse.ArgumentParser(description="三源轨迹归一化入 SQLite")
    ap.add_argument("--lanes", default="lanes", help="源文件目录（默认 lanes）")
    ap.add_argument("--db", default="warehouse.db", help="输出 SQLite（默认 warehouse.db）")
    ap.add_argument("--summary", default="load_summary.txt",
                    help="摘要输出路径（默认 load_summary.txt）")
    args = ap.parse_args(argv)

    paths = {name: os.path.join(args.lanes, name)
             for name in (SH_FILE, DD_FILE, GX_FILE)}
    for name, path in paths.items():
        if not os.path.isfile(path):
            raise SystemExit(f"找不到源文件: {path}")

    sinks = []
    sd = Sink(CAR_SD, SH_FILE)
    sh_units = load_shunda(paths[SH_FILE], sd)
    sinks.append((sd, sh_units, "数据行（不含表头）"))

    dd = Sink(CAR_DD, DD_FILE)
    dd_units = load_diandian(paths[DD_FILE], dd)
    sinks.append((dd, dd_units, "数组元素"))

    gx = Sink(CAR_GX, GX_FILE)
    gx_units = load_ganxian(paths[GX_FILE], gx)
    sinks.append((gx, gx_units, "物理行"))

    all_sinks = [s for s, _, _ in sinks]
    dup_note([r for s in all_sinks for r in s.rejects],
             [e for s in all_sinks for e in s.events])

    stats_rows = []
    for sink, units, label in sinks:
        unknown_n = sum(1 for e in sink.events if not e.code_known)
        note = ""
        if sink.carrier == CAR_GX:
            note = "无 pickup 类事件码，pickup_at 置 NULL"
        stats_rows.append((sink.filename, sink.carrier, label, units,
                           len(sink.events), len(sink.rejects), unknown_n, note))

    n_events, n_rejects, agg = write_db(args.db, all_sinks, stats_rows)
    summary = make_summary(args.db, args.lanes, all_sinks, stats_rows,
                           n_events, n_rejects, agg)
    print(summary)
    with open(args.summary, "w", encoding="utf-8") as fh:
        fh.write(summary + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
