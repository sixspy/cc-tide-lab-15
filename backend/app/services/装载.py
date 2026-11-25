"""观测点装载 → 入窗或迟到。

超阈缓冲由「缓冲溢出策略」模块在同一写事务内裁决：
读阈/数水位与写命运同成败（BEGIN IMMEDIATE 持写锁，并发超阈不双涨）。
"""
from __future__ import annotations
import json
import uuid
from app.db import 连接
from app.seed import 此刻
from app.services import 校验, 时间
from app.modules import 缓冲溢出策略 as 溢出


def 装一批(体: dict) -> dict:
    批次 = 体.get("批次编号") or f"潮批-{uuid.uuid4().hex[:8]}"
    事件 = 体.get("事件") or []
    报告 = {
        "批次编号": 批次, "入流": 0, "入缓冲": 0, "迟到": 0, "驳回": 0,
        "归档": 0, "明细": [],
    }
    for e in 校验.校验批次(体):
        报告["明细"].append({"命运": "批次预检", "说明": e})
    conn = 连接()
    try:
        # 写事务立即拿保留锁：读阈→数水位→写命运同事务，并发装载串行化，不双成功
        conn.execute("BEGIN IMMEDIATE")
        # 窗规在写事务内读：阈值读与命运写同成败，不被并发改窗规撕开
        规 = {r["键"]: r["值"] for r in conn.execute("SELECT 键,值 FROM 窗规").fetchall()}
        窗号 = 规.get("当前窗号", "W-demo")
        窗起 = 规.get("窗起", "2026-11-24T00:00:00Z")
        窗止 = 规.get("窗止", "2026-11-24T01:00:00Z")
        窗内点: list[dict] = []
        for ev in 事件:
            行错 = 校验.校验事件(ev)
            if 行错:
                报告["驳回"] += 1
                报告["明细"].append({"命运": "驳回-校验", "说明": ";".join(行错)})
                continue
            编号 = ev["事件编号"]
            if conn.execute("SELECT 1 FROM 观测点 WHERE 事件编号=?", (编号,)).fetchone():
                报告["驳回"] += 1
                报告["明细"].append({"命运": "驳回-幂等", "说明": 编号})
                continue
            处理 = 此刻()
            conn.execute(
                """INSERT INTO 观测点(事件编号,运单键,网点键,事件时,处理时,命运,批次编号)
                   VALUES (?,?,?,?,?,?,?)""",
                (编号, ev["运单键"], ev["网点键"], ev["事件时"], 处理, "已装载", 批次),
            )
            报告["入流"] += 1
            if 时间.在窗内(ev["事件时"], 窗起, 窗止):
                窗内点.append(ev)
            else:
                # 迟到分轨：出窗点进迟到匣，永不参与缓冲水位，不与溢出点混计
                conn.execute(
                    """INSERT INTO 迟到匣(事件编号,运单键,原因,事件时,入匣时)
                       VALUES (?,?,?,?,?)""",
                    (编号, ev["运单键"], "事件时出窗", ev["事件时"], 处理),
                )
                conn.execute("UPDATE 观测点 SET 命运=? WHERE 事件编号=?", ("已迟到", 编号))
                报告["迟到"] += 1
                报告["明细"].append({"命运": "迟到", "事件编号": 编号})

        # 整批窗内点一次裁决：切流拒整票旁路，双写归档额满即溢，均在本事务内落账
        裁决 = 溢出.裁决窗内点(conn, 窗号, 批次, 窗内点, 规)
        溢出编号 = {x["事件编号"] for x in 裁决["溢出"]}
        处理 = 此刻()
        for ev in 窗内点:
            编号 = ev["事件编号"]
            if 编号 in 溢出编号:
                continue  # 已由模块写入溢出裁决账，现行缓冲不增
            conn.execute(
                """INSERT INTO 入窗缓冲(事件编号,运单键,网点键,事件时,窗号,命运,入缓冲时)
                   VALUES (?,?,?,?,?,?,?)""",
                (编号, ev["运单键"], ev["网点键"], ev["事件时"], 窗号, "在缓冲", 处理),
            )
            conn.execute("UPDATE 观测点 SET 命运=? WHERE 事件编号=?", ("已入缓冲", 编号))
            报告["入缓冲"] += 1
            报告["明细"].append({"命运": "入缓冲", "事件编号": 编号, "窗号": 窗号})
        for d in 裁决["溢出"]:
            编号 = d["事件编号"]
            观测命运 = "已拒溢出" if d["命运"] == 溢出.拒收命运 else "已溢归档"
            conn.execute("UPDATE 观测点 SET 命运=? WHERE 事件编号=?", (观测命运, 编号))
            报告["驳回" if d["命运"] == 溢出.拒收命运 else "归档"] += 1
            报告["明细"].append({
                "命运": "驳回-溢出" if d["命运"] == 溢出.拒收命运 else "归档-溢出",
                "事件编号": 编号, "窗号": 窗号,
                "拒因": d["拒因"], "归档键": d["归档键"], "水位指纹": d["水位指纹"],
            })
        if 裁决["溢出"]:
            报告["溢出裁决"] = {
                "策略": 裁决["策略"], "阈值": 裁决["阈值"],
                "拒因": 裁决["拒因"], "水位指纹": 裁决["指纹"],
                "裁决前现行条数": 裁决["现行条数"],
                "溢出点数": len(裁决["溢出"]),
            }
        conn.execute(
            """INSERT INTO 装载批次(编号,时刻,入流,入缓冲,迟到,报告) VALUES (?,?,?,?,?,?)""",
            (批次, 此刻(), 报告["入流"], 报告["入缓冲"], 报告["迟到"],
             json.dumps(报告, ensure_ascii=False)),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 报告
