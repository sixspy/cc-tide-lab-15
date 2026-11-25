"""缓冲溢出策略：切流拒 / 双写归档 / 并发不双涨 / 水位指纹 / 只读裁决接口。"""
from pathlib import Path
import os
import tempfile
import threading

import pytest

_tmp = tempfile.mkdtemp()
os.environ["数据目录"] = _tmp
os.environ["夹具目录"] = str(Path(__file__).resolve().parents[2] / "fixtures")

from app import seed
from app.db import 库路径
from app.services import 装载, 关窗, 贯验, 目录, 夹具
from app.modules import 缓冲溢出策略 as 溢出模块


@pytest.fixture(autouse=True)
def _db():
    # 每个用例一只新库：窗规阈值/水位不串案
    库路径.unlink(missing_ok=True)
    seed.初始化库()
    yield


def _窗内事件(编号: str, 运单: str = "运单-X", 分: int = 5) -> dict:
    return {
        "事件编号": 编号,
        "运单键": 运单,
        "网点键": "网点-东站",
        "事件时": f"2026-11-24T00:{分:02d}:00Z",
    }


def _设阈(阈: int, 策略: str) -> None:
    目录.改窗规({"入窗缓冲阈值": str(阈), "缓冲溢出策略": 策略})


# ---------- 夹具：阈内成片 ----------

def test_阈内成片基线不被改坏():
    _设阈(10, "切流拒")
    体 = 夹具.读取("窗内成片样例")
    报告 = 装载.装一批(体)
    assert 报告["入缓冲"] == 3
    assert 报告["驳回"] == 0
    关 = 关窗.关当前窗()
    assert 关["成片数"] == 2
    assert 贯验.跑一次()["是否对齐"] is True


def test_无阈值默认照单全收():
    # 不写任何窗规阈值：基线 happy path，空桩不拦
    报告 = 装载.装一批({"事件": [_窗内事件(f"n-{i}", 分=i + 1) for i in range(8)]})
    assert 报告["入缓冲"] == 8
    裁决 = 溢出模块.裁决只读()
    assert 裁决["valid"] is True
    assert 裁决["现行条数"] == 8


def test_夹具灌到超阈_两种策略均可贯验():
    _设阈(4, "切流拒")
    报告 = 装载.装一批(夹具.读取("超阈样例"))
    # 6 点整票超阈：全拒并钉指纹，现行缓冲零增长
    assert 报告["入缓冲"] == 0
    assert 报告["驳回"] == 6
    assert 目录.概览()["在缓冲"] == 0
    assert all(d.get("水位指纹") for d in 报告["明细"] if d["命运"] == "驳回-溢出")
    assert 贯验.跑一次()["是否对齐"] is True

    # 换库换策略：双写归档下阈内 4 进缓冲、2 进只读归档
    库路径.unlink(missing_ok=True)
    seed.初始化库()
    _设阈(4, "双写归档")
    报告2 = 装载.装一批(夹具.读取("超阈样例"))
    assert 报告2["入缓冲"] == 4
    assert 报告2["归档"] == 2
    assert 目录.概览()["在缓冲"] == 4
    归档事件 = {r["事件编号"] for r in 溢出模块.溢出账列表()}
    assert 归档事件.isdisjoint({r["事件编号"] for r in 目录.缓冲列表()})
    assert 贯验.跑一次()["是否对齐"] is True


# ---------- 切流账册拒溢出：超阈整票拒并钉水位指纹，现行缓冲不增 ----------

def test_切流拒_超阈整票拒且缓冲不涨():
    _设阈(5, "切流拒")
    先 = 装载.装一批({"批次编号": "b1", "事件": [_窗内事件(f"a-{i}", 分=i + 1) for i in range(5)]})
    assert 先["入缓冲"] == 5
    水位前 = 目录.概览()["在缓冲"]
    assert 水位前 == 5

    超 = 装载.装一批({"批次编号": "b2", "事件": [_窗内事件(f"b-{i}", 分=10 + i) for i in range(3)]})
    # 整票拒：没有一条混进现行缓冲
    assert 超["入缓冲"] == 0
    assert 超["驳回"] == 3
    assert 目录.概览()["在缓冲"] == 水位前  # 现行缓冲条数不涨
    缓冲事件 = {r["事件编号"] for r in 目录.缓冲列表()}
    assert not any(x.startswith("b-") for x in 缓冲事件)
    # 拒因与水位指纹可钉
    for d in 超["明细"]:
        if d["命运"] == "驳回-溢出":
            assert d["拒因"]
            assert d["水位指纹"]
    裁决 = 溢出模块.裁决只读()
    assert 裁决["valid"] is False
    assert 裁决["阈值"] == 5
    assert 裁决["现行条数"] == 5
    assert 裁决["策略"] == "切流拒"
    assert 裁决["拒因"]
    assert 裁决["水位指纹"]
    账 = 溢出模块.溢出账列表()
    assert len(账) == 3
    assert all(r["命运"] == "溢出拒收" and r["水位指纹"] for r in 账)


def test_切流拒_恰好到阈不算溢出():
    _设阈(3, "切流拒")
    报告 = 装载.装一批({"事件": [_窗内事件(f"e-{i}", 分=i + 1) for i in range(3)]})
    assert 报告["入缓冲"] == 3
    assert 报告["驳回"] == 0
    assert 溢出模块.裁决只读()["valid"] is True


# ---------- 双写两轨：溢出点进只读归档旁路，现行缓冲只读阈内集 ----------

def test_双写_阈内进缓冲溢出点进归档且缓冲不涨():
    _设阈(4, "双写归档")
    装载.装一批({"批次编号": "d1", "事件": [_窗内事件(f"p-{i}", 分=i + 1) for i in range(3)]})
    水位前 = 目录.概览()["在缓冲"]
    assert 水位前 == 3

    报告 = 装载.装一批({"批次编号": "d2", "事件": [_窗内事件(f"q-{i}", 分=10 + i) for i in range(4)]})
    # 只剩 1 个名额；3 个溢出点走归档，现行缓冲只涨 1
    assert 报告["入缓冲"] == 1
    assert 报告["归档"] == 3
    assert 目录.概览()["在缓冲"] == 4  # 卡在阈值，不越线
    缓冲事件 = {r["事件编号"] for r in 目录.缓冲列表()}
    归档 = 溢出模块.溢出账列表()
    归档事件 = {r["事件编号"] for r in 归档}
    assert 归档事件 == {"q-1", "q-2", "q-3"}
    # 两轨读引用不同：归档点不得出现在现行缓冲
    assert 归档事件.isdisjoint(缓冲事件)
    assert all(r["命运"] == "溢入归档" and r["归档键"] and r["水位指纹"] for r in 归档)
    裁决 = 溢出模块.裁决只读()
    assert 裁决["valid"] is False
    assert 裁决["阈值"] == 4
    assert 裁决["现行条数"] == 4
    assert 裁决["归档键"]


def test_双写_整票都溢时缓冲零增长():
    _设阈(2, "双写归档")
    装载.装一批({"事件": [_窗内事件(f"z-{i}", 分=i + 1) for i in range(2)]})
    水位前 = 目录.概览()["在缓冲"]
    报告 = 装载.装一批({"事件": [_窗内事件(f"w-{i}", 分=10 + i) for i in range(2)]})
    assert 报告["入缓冲"] == 0
    assert 报告["归档"] == 2
    assert 目录.概览()["在缓冲"] == 水位前


# ---------- 迟到匣与溢出点不混计 ----------

def test_溢出点与窗内点迟到点不混计():
    _设阈(2, "双写归档")
    报告 = 装载.装一批({
        "批次编号": "mix",
        "事件": [
            _窗内事件("m-in-1", 分=1),
            _窗内事件("m-in-2", 分=2),
            _窗内事件("m-in-3", 分=3),       # 溢出 → 归档
            {  # 出窗 → 迟到匣
                "事件编号": "m-late", "运单键": "运单-L", "网点键": "网点-东站",
                "事件时": "2026-11-24T03:00:00Z",
            },
        ],
    })
    assert 报告["入缓冲"] == 2
    assert 报告["归档"] == 1
    assert 报告["迟到"] == 1
    迟到事件 = {r["事件编号"] for r in 目录.迟到列表()}
    归档事件 = {r["事件编号"] for r in 溢出模块.溢出账列表()}
    assert "m-late" in 迟到事件
    assert "m-in-3" in 归档事件
    assert 迟到事件.isdisjoint(归档事件)


# ---------- 并发超阈不得双涨：不加锁会双成功 ----------

def test_并发超阈不得双涨_切流拒():
    _设阈(5, "切流拒")
    # 仅剩 3 个名额恰好够一批：赢锁者装满到阈，输锁者整票拒，绝不双成功
    装载.装一批({"事件": [_窗内事件(f"seed-{i}", 分=i + 1) for i in range(2)]})
    果: list[dict] = []
    锁 = threading.Lock()

    def  worker(前缀: str):
        r = 装载.装一批({"事件": [_窗内事件(f"{前缀}-{i}", 分=20 + i) for i in range(3)]})
        with 锁:
            果.append(r)

    t1 = threading.Thread(target=worker, args=("t1",))
    t2 = threading.Thread(target=worker, args=("t2",))
    t1.start(); t2.start(); t1.join(); t2.join()

    # 两批各 3 点抢 2 个名额：整票拒形态下只有一批能成，另一批整票拒；绝不双成功
    成功 = [r for r in 果 if r["入缓冲"] == 3]
    拒批 = [r for r in 果 if r["入缓冲"] == 0 and r["驳回"] == 3]
    assert len(成功) == 1
    assert len(拒批) == 1
    assert 目录.概览()["在缓冲"] == 5  # 水位恰好钉在阈值，不双涨
    裁决 = 溢出模块.裁决只读()
    assert 裁决["现行条数"] == 5
    assert 裁决["valid"] is False
    assert len(溢出模块.溢出账列表()) == 3  # 仅被拒那批留指纹账


def test_并发超阈不得双涨_双写归档():
    _设阈(5, "双写归档")
    装载.装一批({"事件": [_窗内事件(f"seed2-{i}", 分=i + 1) for i in range(3)]})
    果: list[dict] = []
    锁 = threading.Lock()

    def worker(前缀: str):
        r = 装载.装一批({"事件": [_窗内事件(f"{前缀}-{i}", 分=20 + i) for i in range(3)]})
        with 锁:
            果.append(r)

    t1 = threading.Thread(target=worker, args=("c1",))
    t2 = threading.Thread(target=worker, args=("c2",))
    t1.start(); t2.start(); t1.join(); t2.join()

    # 只剩 2 个名额：两批合计入缓冲必须恰好 2，其余 4 个全归档，绝不越过阈值
    入 = sum(r["入缓冲"] for r in 果)
    档 = sum(r["归档"] for r in 果)
    assert 入 == 2
    assert 档 == 4
    assert 目录.概览()["在缓冲"] == 5
    归档事件 = [r["事件编号"] for r in 溢出模块.溢出账列表()]
    assert len(归档事件) == len(set(归档事件))  # 归档键不重


# ---------- 收尾：窗规装载缓冲迟到匣贯验 ----------

def test_溢出后关窗贯验仍对齐():
    _设阈(3, "切流拒")
    装载.装一批({"事件": [_窗内事件(f"v-{i}", 分=i + 1) for i in range(3)]})
    装载.装一批({"事件": [_窗内事件(f"o-{i}", 分=10 + i) for i in range(2)]})  # 整票拒
    关 = 关窗.关当前窗()
    assert 关["缓冲消耗"] == 3
    验 = 贯验.跑一次()
    assert 验["是否对齐"] is True
    # 拒收点不进切片账
    assert 验["切片事件"] == 3
