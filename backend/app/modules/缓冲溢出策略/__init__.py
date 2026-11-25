"""缓冲溢出策略（邻题落地，替换空桩）。

两种裁决形态，窗规同票：
- 切流拒（默认）：超阈整票拒。本批窗内点一条都不进现行缓冲，全部入溢出裁决账，
  命运「溢出拒收」并钉水位指纹（拒因+指纹）。现行缓冲条数不涨。
- 双写归档：阈内点进现行缓冲，溢出点进只读归档旁路（溢出裁决账，命运「溢入归档」）。
  现行缓冲只读阈内集，归档与缓冲读引用不同，现行缓冲永远不越阈值。

硬约束：
- 读阈/数水位与写命运必须在同一个写事务内完成（装载服务以 BEGIN IMMEDIATE 开事务），
  并发超阈不得双成功、双涨。
- 溢出点与窗内点、迟到点分轨，不混计。
"""
from __future__ import annotations
import hashlib
import uuid
from app.db import 连接
from app.seed import 此刻

NAME = "缓冲溢出策略"
CONTRACT = "缓冲条数超阈值时拒装或溢入旁路"

HOOKS = (
    "before_load",
    "after_load",
    "decide_window",
    "before_close",
    "after_close",
    "extra_verify_checks",
)

策略_切流拒 = "切流拒"
策略_双写归档 = "双写归档"
策略集 = (策略_切流拒, 策略_双写归档)

拒收命运 = "溢出拒收"
归档命运 = "溢入归档"


def describe() -> dict:
    return {
        "模块": NAME,
        "契约": CONTRACT,
        "钩子": list(HOOKS),
        "默认启用": True,
        "策略": list(策略集),
        "窗规键": ("入窗缓冲阈值", "缓冲溢出策略"),
        "说明": "阈值 0 表示不限（基线照单全收）；超阈按策略整票拒或溢入只读归档",
    }


def 解析阈值(规: dict) -> int:
    """窗规阈值；缺失/0/非正数一律视为不限。"""
    try:
        v = int(str(规.get("入窗缓冲阈值", "0")).strip() or "0")
    except (TypeError, ValueError):
        return 0
    return max(v, 0)


def 解析策略(规: dict) -> str:
    v = str(规.get("缓冲溢出策略", 策略_切流拒) or 策略_切流拒).strip()
    return v if v in 策略集 else 策略_切流拒


def 水位指纹(窗号: str, 阈值: int, 现行条数: int, 批次编号: str) -> str:
    """钉住裁决瞬间的水位：窗号/阈值/现行条数/批次，哈希成短指纹。"""
    料 = f"{窗号}|{阈值}|{现行条数}|{批次编号}|{此刻()}"
    return "FP-" + hashlib.sha256(料.encode("utf-8")).hexdigest()[:16]


def 现行条数(conn, 窗号: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 窗号=? AND 命运='在缓冲'",
        (窗号,),
    ).fetchone()["c"]


def 裁决窗内点(
    conn,
    窗号: str,
    批次编号: str,
    窗内点: list[dict],
    规: dict,
) -> dict:
    """在调用方的写事务内裁决一批窗内点。

    返回 {"入缓冲": [...], "溢出": [...], "阈值", "策略", "指纹", "拒因"}。
    调用方据此分别写 入窗缓冲 / 溢出裁决账；本函数只做裁决与旁路落账，
    读阈、数水位、写命运同事务，杜绝并发双涨。
    """
    阈值 = 解析阈值(规)
    策略 = 解析策略(规)
    条数 = 现行条数(conn, 窗号)
    指纹 = 水位指纹(窗号, 阈值, 条数, 批次编号)
    空: dict = {"入缓冲": [], "溢出": [], "阈值": 阈值, "策略": 策略,
                "指纹": 指纹, "拒因": "", "现行条数": 条数}
    if not 窗内点 or 阈值 == 0:
        return 空
    if 条数 >= 阈值:
        拒因 = f"缓冲水位{条数}已达阈值{阈值}（窗{窗号}）"
    elif 条数 + len(窗内点) > 阈值:
        拒因 = f"本批{len(窗内点)}点将越阈：现行{条数}+{len(窗内点)}>{阈值}（窗{窗号}）"
    else:
        return 空  # 阈内，照单全收，不产生指纹账

    if 策略 == 策略_切流拒:
        # 超阈整票拒：窗内点全部旁路，现行缓冲一个不增
        溢出 = [
            _落账(conn, 批次编号, ev, 窗号, 策略, 拒收命运, 拒因, 指纹)
            for ev in 窗内点
        ]
        return {**空, "溢出": 溢出, "拒因": 拒因}

    # 双写归档：只剩 (阈值-条数) 个名额，额满即溢
    名额 = max(阈值 - 条数, 0)
    入缓冲, 溢出 = 窗内点[:名额], 窗内点[名额:]
    归档 = [
        _落账(conn, 批次编号, ev, 窗号, 策略, 归档命运, 拒因, 指纹)
        for ev in 溢出
    ]
    return {**空, "入缓冲": 入缓冲, "溢出": 归档, "拒因": 拒因}


def _落账(conn, 批次编号: str, ev: dict, 窗号: str, 策略: str,
          命运: str, 拒因: str, 指纹: str) -> dict:
    归档键 = f"ARC-{窗号}-{ev['事件编号']}-{uuid.uuid4().hex[:8]}"
    conn.execute(
        """INSERT INTO 溢出裁决账(归档键,批次编号,事件编号,运单键,网点键,事件时,
               窗号,策略,命运,拒因,水位指纹,入册时)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (归档键, 批次编号, ev["事件编号"], ev["运单键"], ev["网点键"],
         ev["事件时"], 窗号, 策略, 命运, 拒因, 指纹, 此刻()),
    )
    return {
        "事件编号": ev["事件编号"], "归档键": 归档键, "命运": 命运,
        "拒因": 拒因, "水位指纹": 指纹,
    }


def 裁决只读(窗号: str | None = None) -> dict:
    """新增溢出裁决只读接口：阈值、现行条数、拒因或归档键、valid。"""
    conn = 连接()
    try:
        规 = {r["键"]: r["值"] for r in conn.execute("SELECT 键,值 FROM 窗规").fetchall()}
        号 = 窗号 or 规.get("当前窗号", "W-demo")
        阈值 = 解析阈值(规)
        策略 = 解析策略(规)
        条数 = 现行条数(conn, 号)
        最新 = conn.execute(
            "SELECT * FROM 溢出裁决账 WHERE 窗号=? ORDER BY 编号 DESC LIMIT 1",
            (号,),
        ).fetchone()
        # valid：未越阈且本窗没有产生过溢出裁决（拒收/归档都算超阈命运）
        valid = (阈值 == 0 or 条数 <= 阈值) and 最新 is None
        # 只读接口按命运给「拒因或归档键」：切流拒看拒因，双写归档看归档键
        if 最新 is None:
            拒因, 归档键, 指纹 = "", "", ""
        elif 最新["命运"] == 拒收命运:
            拒因, 归档键, 指纹 = 最新["拒因"], "", 最新["水位指纹"]
        else:
            拒因, 归档键, 指纹 = "", 最新["归档键"], 最新["水位指纹"]
        out = {
            "窗号": 号,
            "阈值": 阈值,
            "策略": 策略,
            "现行条数": 条数,
            "valid": bool(valid),
            "拒因": 拒因,
            "归档键": 归档键,
            "水位指纹": 指纹,
        }
        return out
    finally:
        conn.close()


def 溢出账列表(限制: int = 200) -> list[dict]:
    """只读归档旁路：与入窗缓冲读引用不同，溢出点不进现行缓冲列表。"""
    conn = 连接()
    try:
        rows = conn.execute(
            "SELECT * FROM 溢出裁决账 ORDER BY 编号 DESC LIMIT ?", (限制,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---- 空桩钩子面保留：基线默认透传，真实接线在装载服务 ----

def before_load(batch: dict) -> dict:
    """装载前：可改批次或抛出预检错误。默认透传。"""
    return batch


def after_load(report: dict) -> dict:
    """装载后：可附加旁路统计。默认透传。"""
    return report


def decide_window(event: dict, rules: dict) -> str:
    """返回 入缓冲 / 迟到 / 驳回。基线默认由装载服务按事件时判定。"""
    return "入缓冲"


def before_close(window_id: str) -> None:
    """关窗前检查，可抛错阻断。"""
    return None


def after_close(report: dict) -> dict:
    """关窗后处理，例如推进下一窗。默认透传。"""
    return report


def extra_verify_checks(snapshot: dict) -> list[str]:
    """附加贯验检查：现行缓冲不得越阈；返回错误文案列表，空列表表示通过。"""
    阈值 = 解析阈值(snapshot or {})
    条数 = int((snapshot or {}).get("现行条数", 0) or 0)
    if 阈值 and 条数 > 阈值:
        return [f"缓冲越阈：现行{条数}>阈值{阈值}"]
    return []
