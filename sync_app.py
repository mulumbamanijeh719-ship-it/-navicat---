#!/usr/bin/env python3
"""
MySQL 通用增量数据同步工具 — GUI 版

支持三种同步模式（在 GUI 中一键切换）:
  【pk】      主键模式 — 表有主键/唯一键，用 ON DUPLICATE KEY UPDATE 去重
  【append】  追加模式 — 无主键流水表，纯 INSERT 追加
  【row_match】行匹配模式 — 无主键但指定业务列组合来定位行，DELETE+INSERT 实现"更新"

依赖: pip install pymysql
运行: venv/Scripts/python.exe sync_app.py  或  双击 run.bat
"""

from __future__ import annotations

import json, os, queue, sys, threading, time, tkinter as tk
from datetime import datetime, timedelta
from tkinter import messagebox, scrolledtext, ttk

# ---- 驱动 ----
try:
    import pymysql; MySQL = pymysql
except ImportError:
    sys.exit("缺少 pymysql 驱动，请执行: pip install pymysql")
try:
    _SSCursor = MySQL.cursors.SSCursor
except AttributeError:
    _SSCursor = MySQL.cursors.Cursor

# ---- 常量 ----
# 路径兼容：开发时用脚本目录，打包成exe后用exe所在目录
def _app_dir():
    if getattr(sys,'frozen',False): return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(_app_dir(), "sync_config.json")
CHECKPOINT_FILE = os.path.join(_app_dir(), "sync_checkpoint.json")
DEFAULT_CONFIG = {
    "source":      {"host":"127.0.0.1","port":3306,"user":"root","password":"","database":"","table":""},
    "target":      {"host":"127.0.0.1","port":3306,"user":"root","password":"","database":"","table":""},
    "sync": {
        "sync_mode":"pk",                # pk(主键) / append(追加) / row_match(行匹配)
        "change_field":"updated_at",     # 变化检测 DATETIME 列
        "primary_keys":"id",             # pk 模式：主键列
        "match_columns":"",              # row_match 模式：匹配列(逗号分隔)
        "interval_seconds":300,
        "batch_size":500,
        "watermark_delay_seconds":30,
        "snapshot_chunk_size":10000,     # pk 模式：Snapshot 分片行数
    },
    "field_mapping":{},
}

# ===========================================================================
# 配置管理
# ===========================================================================
class ConfigManager:
    @staticmethod
    def load():
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE,"r",encoding="utf-8") as f: cfg=json.load(f)
            r={}
            for k,v in DEFAULT_CONFIG.items():
                if isinstance(v,dict) and isinstance(cfg.get(k),dict): r[k]={**v,**cfg[k]}
                else: r[k]=cfg.get(k,v)
            return r
        return dict(DEFAULT_CONFIG)
    @staticmethod
    def save(cfg):
        with open(CONFIG_FILE,"w",encoding="utf-8") as f: json.dump(cfg,f,indent=2,ensure_ascii=False)

# ===========================================================================
# 数据库辅助
# ===========================================================================
class DBHelper:
    @staticmethod
    def connect(info,db=None):
        d=db or info.get("database","")
        return MySQL.connect(host=info["host"],port=info["port"],user=info["user"],password=info["password"],database=d,charset="utf8mb4",cursorclass=MySQL.cursors.Cursor)
    @staticmethod
    def test(info):
        try:
            c=DBHelper.connect(info); c.ping(); c.close(); return True,"连接成功"
        except Exception as e: return False,str(e)
    @staticmethod
    def get_columns(info,db,table):
        c=DBHelper.connect(info,db)
        try:
            with c.cursor() as cur:
                cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",(db,table))
                return [r[0] for r in cur.fetchall()]
        finally: c.close()
    @staticmethod
    def table_exists(info,db,table):
        c=DBHelper.connect(info,db)
        try:
            with c.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",(db,table))
                return cur.fetchone()[0]>0
        finally: c.close()
    @staticmethod
    def create_table_from_source(tgt_info,src_db,src_tbl,tgt_db,tgt_tbl):
        c=DBHelper.connect(tgt_info,tgt_db)
        try:
            with c.cursor() as cur: cur.execute(f"CREATE TABLE `{tgt_db}`.`{tgt_tbl}` LIKE `{src_db}`.`{src_tbl}`")
            c.commit()
        finally: c.close()
    @staticmethod
    def get_pk_min_max(info,db,table,pk):
        c=DBHelper.connect(info,db)
        try:
            with c.cursor() as cur:
                cur.execute(f"SELECT MIN(`{pk}`),MAX(`{pk}`) FROM `{db}`.`{table}`")
                r=cur.fetchone()
                if r and r[0] is not None:
                    try: return int(r[0]),int(r[1])
                    except (ValueError,TypeError): return None
                return None
        finally: c.close()

# ===========================================================================
# 统一同步引擎 (pk / append / row_match)
# ===========================================================================
class SyncEngine:
    def __init__(self,cfg,log_queue):
        self.cfg=cfg; self.log=log_queue; self._stop_flag=threading.Event()

    def _info(self,msg): self.log.put(("INFO",msg))
    def _warn(self,msg): self.log.put(("WARN",msg))
    def _error(self,msg): self.log.put(("ERROR",msg))

    # ---- Checkpoint（存在本地文件，不在数据库建表）----
    def _load_checkpoint(self):
        try:
            with open(CHECKPOINT_FILE,"r",encoding="utf-8") as f: cp=json.load(f)
            # 兼容旧格式：last_sync 可能是字符串
            if isinstance(cp.get("last_sync"),str):
                cp["last_sync"]=datetime.strptime(cp["last_sync"],"%Y-%m-%d %H:%M:%S")
            return cp
        except: pass
        return {"phase":"snapshot","last_sync":datetime(1970,1,1),"snap":0,"incr":0,"ins":0,"upd":0}

    def _save_checkpoint(self,**kw):
        cp=self._load_checkpoint()
        # 兼容旧 MySQL 字段名 → 内存字段名
        km={"sync_phase":"phase","last_sync_time":"last_sync","total_snapshotted":"snap","total_increment":"incr","total_inserted":"ins","total_updated":"upd"}
        for k,v in kw.items():
            key=km.get(k,k)  # 旧名映射到新名，新名保持不变
            cp[key]=v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v,datetime) else v
        with open(CHECKPOINT_FILE,"w",encoding="utf-8") as f: json.dump(cp,f,indent=2,ensure_ascii=False)

    def _reset_checkpoint(self):
        cp={"phase":"snapshot","last_sync":"1970-01-01 00:00:00","snap":0,"incr":0,"ins":0,"upd":0}
        with open(CHECKPOINT_FILE,"w",encoding="utf-8") as f: json.dump(cp,f,indent=2,ensure_ascii=False)
        self._info("Checkpoint 已重置 -> 下次运行执行全量 Snapshot")

    # ---- 字段映射 ----
    def _build_field_map(self,sc,tc):
        m=self.cfg.get("field_mapping",{})
        if m: return m
        ts=set(tc); return {c:c for c in sc if c in ts}

    # ---- Watermark ----
    def _calc_cutoff(self):
        d=self.cfg["sync"].get("watermark_delay_seconds",30)
        return datetime.now()-timedelta(seconds=d)

    # ==================================================================
    # 主入口
    # ==================================================================
    def run_once(self):
        t0=datetime.now(); mode=self.cfg["sync"].get("sync_mode","pk")
        self._info("="*50); self._info(f"CDC 同步开始 (模式:{mode})")
        src_conn=tgt_conn=None
        try:
            sc=self.cfg["source"]; tc=self.cfg["target"]
            src_conn=DBHelper.connect(sc)
            # 目标表不存在则自动创建
            if not DBHelper.table_exists(tc,tc["database"],tc["table"]):
                self._info(f"目标表不存在，自动创建...")
                DBHelper.create_table_from_source(tc,sc["database"],sc["table"],tc["database"],tc["table"])
                self._info("目标表已创建")
            tgt_conn=DBHelper.connect(tc)
            cp=self._load_checkpoint()
            self._info(f"阶段:{cp['phase']} | 上次同步:{cp['last_sync']}")
            # 列映射
            scols=DBHelper.get_columns(sc,sc["database"],sc["table"])
            tcols=DBHelper.get_columns(tc,tc["database"],tc["table"])
            fm=self._build_field_map(scols,tcols)
            if not fm: self._error("字段映射为空"); return
            cf=self.cfg["sync"].get("change_field","")
            if not cf.strip(): self._error("变化检测字段不能为空"); return
            self._info(f"字段映射:{fm}")
            # 分阶段
            if cp["phase"]=="snapshot": self._run_snapshot(src_conn,tgt_conn,fm,cp)
            else: self._run_incremental(src_conn,tgt_conn,fm,cp)
        except Exception as e:
            self._error(f"同步异常:{e}")
        finally:
            for c in (src_conn,tgt_conn):
                if c:
                    try: c.close()
                    except: pass
        self._info(f"同步完成 (耗时 {(datetime.now()-t0).total_seconds():.1f}s)"); self._info("="*50)

    # ==================================================================
    # Snapshot（根据模式不同策略）
    # ==================================================================
    def _run_snapshot(self,src,tgt,fm,cp):
        mode=self.cfg["sync"].get("sync_mode","pk")
        if mode=="pk": self._snapshot_pk(src,tgt,fm,cp)
        else: self._snapshot_append(src,tgt,fm,cp)  # append / row_match 都用追加式全量

    def _snapshot_pk(self,src,tgt,fm,cp):
        """PK 模式全量：数字PK按范围分chunk，字符串PK用LIMIT/OFFSET分页"""
        sc=self.cfg["source"]; tc=self.cfg["target"]; sy=self.cfg["sync"]
        pk_str=sy["primary_keys"]; pk_col=pk_str.split(",")[0].strip()
        chunk_size=sy.get("snapshot_chunk_size",10000)
        st=f"`{sc['database']}`.`{sc['table']}`"; tt=f"`{tc['database']}`.`{tc['table']}`"
        self._info(">>> [Snapshot/pk] 全量快照 <<<")
        # 源表行数
        with src.cursor() as cur: cur.execute(f"SELECT COUNT(*) FROM {st}"); src_total=cur.fetchone()[0]
        if src_total==0: self._save_checkpoint(sync_phase="incremental"); self._info("源表为空"); return
        self._info(f"源表:{src_total}行")
        # 构建SQL
        ms=[c for c in fm.keys()]
        scs=", ".join(f"`{c}`" for c in ms); tcs=", ".join(f"`{fm[c]}`" for c in ms)
        pkl=[k.strip() for k in pk_str.split(",")]
        uc=", ".join(f"`{fm[c]}`=VALUES(`{fm[c]}`)" for c in ms if c not in pkl)
        us=f" ON DUPLICATE KEY UPDATE {uc}" if uc else ""
        base_sql=f"INSERT INTO {tt} ({tcs}) SELECT {scs} FROM {st}"
        # 判断主键类型
        pk_range=DBHelper.get_pk_min_max(sc,sc["database"],sc["table"],pk_col)
        tsnap=0; cp_time=None
        if pk_range:
            # 数字主键 → 范围分chunk（高效）
            pk_min,pk_max=pk_range; chunks=((pk_max-pk_min)//chunk_size)+1
            self._info(f"数字PK {pk_min}~{pk_max}, {chunks} chunks")
            for ci,cs in enumerate(range(pk_min,pk_max+1,chunk_size)):
                if self._stop_flag.is_set(): self._warn("Snapshot 中断"); return
                ce=min(cs+chunk_size,pk_max+1)
                sql=f"INSERT INTO {tt} ({tcs}) SELECT {scs} FROM {st} WHERE `{pk_col}`>={cs} AND `{pk_col}`<{ce}{us}"
                with tgt.cursor() as cur: cur.execute(sql); aff=cur.rowcount
                tgt.commit(); tsnap+=aff; cp_time=datetime.now()
                self._info(f"  chunk [{ci+1}/{chunks}] pk {cs}~{ce} | {aff}行")
                self._save_checkpoint(last_sync_time=cp_time,total_snapshotted=cp["snap"]+tsnap)
        else:
            # 字符串主键 → LIMIT/OFFSET 分页
            self._info(f"字符串PK，使用分页方式")
            bs=chunk_size; off=0
            while True:
                if self._stop_flag.is_set(): self._warn("Snapshot 中断"); return
                sql=f"{base_sql} LIMIT {bs} OFFSET {off}{us}"
                with tgt.cursor() as cur: cur.execute(sql); aff=cur.rowcount
                tgt.commit(); tsnap+=aff; off+=bs; cp_time=datetime.now()
                pct=min(100,int(tsnap/src_total*100))
                self._info(f"  已处理 {tsnap}/{src_total} ({pct}%)")
                self._save_checkpoint(last_sync_time=cp_time,total_snapshotted=cp["snap"]+tsnap)
                if aff<bs: break
        self._save_checkpoint(sync_phase="incremental",last_sync_time=cp_time,total_snapshotted=cp["snap"]+tsnap)
        self._info(f">>> Snapshot 完成: {tsnap}行")

    def _snapshot_append(self,src,tgt,fm,cp):
        """append/row_match 模式全量：TRUNCATE 目标表 + 全量 INSERT...SELECT"""
        sc=self.cfg["source"]; tc=self.cfg["target"]; sy=self.cfg["sync"]
        st=f"`{sc['database']}`.`{sc['table']}`"; tt=f"`{tc['database']}`.`{tc['table']}`"
        self._info(">>> [Snapshot/append] 全量追加 <<<")
        with src.cursor() as cur: cur.execute(f"SELECT COUNT(*) FROM {st}"); src_total=cur.fetchone()[0]
        self._info(f"源表:{src_total}行")
        if src_total==0: self._save_checkpoint(sync_phase="incremental"); self._info("源表为空"); return
        # TRUNCATE
        self._info(f"清空目标表...")
        with tgt.cursor() as cur: cur.execute(f"TRUNCATE TABLE {tt}")
        tgt.commit()
        # INSERT
        ms=[c for c in fm.keys()]
        scs=", ".join(f"`{c}`" for c in ms); tcs=", ".join(f"`{fm[c]}`" for c in ms)
        sql=f"INSERT INTO {tt} ({tcs}) SELECT {scs} FROM {st}"
        bs=sy.get("batch_size",1000); total=0
        offset=0
        while True:
            if self._stop_flag.is_set(): self._warn("Snapshot 中断"); return
            with tgt.cursor() as cur: cur.execute(f"{sql} LIMIT {bs} OFFSET {offset}"); aff=cur.rowcount
            tgt.commit(); total+=aff; offset+=bs
            pct=min(100,int(total/src_total*100)) if src_total else 100
            self._info(f"  已追加 {total}/{src_total} ({pct}%)")
            if aff<bs: break
        stime=datetime.now()
        self._save_checkpoint(sync_phase="incremental",last_sync_time=stime,total_snapshotted=cp["snap"]+total,total_inserted=cp["ins"]+total)
        self._info(f">>> Snapshot 完成: {total}行")
        self._info(f"Snapshot append: {total}行")

    # ==================================================================
    # Incremental（根据模式分发）
    # ==================================================================
    def _run_incremental(self,src,tgt,fm,cp):
        mode=self.cfg["sync"].get("sync_mode","pk")
        sc=self.cfg["source"]; tc=self.cfg["target"]; sy=self.cfg["sync"]
        cf=sy["change_field"]; last_sync=cp["last_sync"]; cutoff=self._calc_cutoff()
        st=f"`{sc['database']}`.`{sc['table']}`"
        self._info(f">>> [Incremental/{mode}] <<<")
        self._info(f"窗口:({last_sync.strftime('%H:%M:%S')},{cutoff.strftime('%H:%M:%S')}] watermark:{sy.get('watermark_delay_seconds',30)}s")
        # 诊断查询
        with src.cursor() as cur:
            cur.execute(f"SELECT MIN(`{cf}`),MAX(`{cf}`) FROM {st}"); r=cur.fetchone()
            src_min,src_max=r[0],r[1]
            cur.execute(f"SELECT COUNT(*) FROM {st} WHERE `{cf}`>%s AND `{cf}`<=%s",(last_sync,cutoff)); total=cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {st} WHERE `{cf}`>%s",(cutoff,)); after=cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {st} WHERE `{cf}`<=%s",(last_sync,)); before=cur.fetchone()[0]
        self._info(f"源表{cf}范围:{src_min} ~ {src_max}")
        self._info(f"Checkpoint:{last_sync.strftime('%Y-%m-%d %H:%M:%S')} Cutoff:{cutoff.strftime('%Y-%m-%d %H:%M:%S')}")
        self._info(f"数据分布: <=checkpoint{before}条 | 待处理{total}条 | >cutoff{after}条")
        if total==0:
            if after>0: self._info("-> 有数据被 Watermark 挡住")
            elif before>0 and after==0: self._info("-> 已全部同步"); self._save_checkpoint(last_sync_time=cutoff)
            else: self._info("-> 源表为空"); self._save_checkpoint(last_sync_time=cutoff)
            return
        if mode=="pk": self._incr_pk(src,tgt,st,fm,last_sync,cutoff,total,cp)
        elif mode=="append": self._incr_append(src,tgt,st,fm,last_sync,cutoff,total,cp)
        elif mode=="row_match": self._incr_row_match(src,tgt,st,fm,last_sync,cutoff,total,cp)

    # ---- PK 模式增量 ----
    def _incr_pk(self,src,tgt,st,fm,ls,co,total,cp):
        sy=self.cfg["sync"]; tc=self.cfg["target"]
        tt=f"`{tc['database']}`.`{tc['table']}`"
        ms=[c for c in fm.keys()]
        scs=", ".join(f"`{c}`" for c in ms); tcs=", ".join(f"`{fm[c]}`" for c in ms)
        pkl=[k.strip() for k in sy["primary_keys"].split(",")]
        uc=", ".join(f"`{fm[c]}`=VALUES(`{fm[c]}`)" for c in ms if c not in pkl)
        us=f" ON DUPLICATE KEY UPDATE {uc}" if uc else ""
        ss=f"INSERT INTO {tt} ({tcs}) SELECT {scs} FROM {st} WHERE `{sy['change_field']}`>%s AND `{sy['change_field']}`<=%s"
        bs=sy.get("batch_size",500); off=0; aff_total=0
        self._info(f"批量同步(每批{bs})")
        while True:
            if self._stop_flag.is_set(): self._warn("中断"); break
            with tgt.cursor() as cur: cur.execute(f"{ss} LIMIT {bs} OFFSET {off}{us}",(ls,co)); aff=cur.rowcount
            tgt.commit(); aff_total+=aff; off+=bs
            if aff<bs: break
        eu=max(0,aff_total-total); ei=total-eu
        self._info(f"增量结果:{aff_total}行 | 新增~{ei} | 覆盖~{eu}")
        self._save_checkpoint(last_sync_time=co,total_increment=cp["incr"]+total,total_inserted=cp["ins"]+ei,total_updated=cp["upd"]+eu)
        self._info(f"Incremental pk: {aff_total}行 insert~{ei} update~{eu}")

    # ---- append 模式增量 ----
    def _incr_append(self,src,tgt,st,fm,ls,co,total,cp):
        sy=self.cfg["sync"]; tc=self.cfg["target"]
        tt=f"`{tc['database']}`.`{tc['table']}`"
        ms=[c for c in fm.keys()]
        scs=", ".join(f"`{c}`" for c in ms); tcs=", ".join(f"`{fm[c]}`" for c in ms)
        ss=f"INSERT INTO {tt} ({tcs}) SELECT {scs} FROM {st} WHERE `{sy['change_field']}`>%s AND `{sy['change_field']}`<=%s"
        bs=sy.get("batch_size",1000); off=0; total_ins=0
        self._info(f"追加增量(每批{bs})")
        while True:
            if self._stop_flag.is_set(): self._warn("中断"); break
            with tgt.cursor() as cur: cur.execute(f"{ss} LIMIT {bs} OFFSET {off}",(ls,co)); aff=cur.rowcount
            tgt.commit(); total_ins+=aff; off+=bs
            if aff<bs: break
        self._info(f"增量结果:追加{total_ins}行")
        self._save_checkpoint(last_sync_time=co,total_increment=cp["incr"]+total,total_inserted=cp["ins"]+total_ins)
        self._info(f"Incremental append: {total_ins}行")

    # ---- row_match 模式增量 ----
    def _incr_row_match(self,src,tgt,st,fm,ls,co,total,cp):
        sy=self.cfg["sync"]; sc=self.cfg["source"]; tc=self.cfg["target"]
        tt=f"`{tc['database']}`.`{tc['table']}`"
        mcs_str=sy.get("match_columns","")
        if not mcs_str.strip(): self._error("row_match 需指定匹配列"); return
        mcs=[c.strip() for c in mcs_str.split(",") if c.strip()]
        ms=[c for c in fm.keys()]
        cs=", ".join(f"`{c}`" for c in ms)
        sq=f"SELECT {cs} FROM {st} WHERE `{sy['change_field']}`>%s AND `{sy['change_field']}`<=%s ORDER BY `{sy['change_field']}` ASC"
        ok=0; err=0
        self._info(f"行匹配同步(匹配列:{mcs})")
        with src.cursor(_SSCursor) as scur:
            scur.execute(sq,(ls,co))
            while True:
                if self._stop_flag.is_set(): self._warn("中断"); break
                row=scur.fetchone()
                if row is None: break
                rd=dict(zip(ms,row))
                try:
                    with tgt.cursor() as tcur:
                        # DELETE
                        dp=[]; dv=[]
                        for c in mcs:
                            tc_c=fm.get(c,c); v=rd.get(c)
                            if v is None: dp.append(f"`{tc_c}` IS NULL")
                            else: dp.append(f"`{tc_c}`=%s"); dv.append(v)
                        if dp: tcur.execute(f"DELETE FROM {tt} WHERE {' AND '.join(dp)}",dv)
                        else: tcur.execute(f"DELETE FROM {tt}")
                        # INSERT
                        tr={fm[k]:v for k,v in rd.items() if k in fm}
                        cn=", ".join(f"`{c}`" for c in tr.keys())
                        ph=", ".join(["%s"]*len(tr))
                        tcur.execute(f"INSERT INTO {tt} ({cn}) VALUES ({ph})",list(tr.values()))
                    tgt.commit(); ok+=1
                except Exception as e:
                    err+=1; self._error(f"跳过:{e}")
                    try: tgt.rollback()
                    except: pass
        self._info(f"增量结果:成功{ok}|失败{err}")
        self._save_checkpoint(last_sync_time=co,total_increment=cp["incr"]+ok,total_inserted=cp["ins"]+ok)
        self._info(f"Incremental row_match: 成功{ok} 失败{err}")

    def stop(self): self._stop_flag.set()

# ===========================================================================
# 调度线程
# ===========================================================================
class SchedulerThread(threading.Thread):
    def __init__(self,cfg,log_queue):
        super().__init__(daemon=True); self.cfg=cfg; self.log_queue=log_queue
        self.engine=SyncEngine(cfg,log_queue); self._paused=threading.Event(); self._stopped=threading.Event()
    def run(self):
        iv=self.cfg["sync"].get("interval_seconds",300)
        self.log_queue.put(("INFO",f"调度器启动,间隔{iv}s"))
        while not self._stopped.is_set():
            self._paused.wait()
            if self._stopped.is_set(): break
            self.engine.run_once()
            for _ in range(iv):
                if self._stopped.is_set(): break
                if not self._paused.is_set(): time.sleep(1)
        self.log_queue.put(("INFO","调度器已停止"))
    def pause(self): self._paused.clear(); self.log_queue.put(("INFO","已暂停"))
    def resume(self): self._paused.set(); self.log_queue.put(("INFO","已恢复"))
    def stop(self): self._stopped.set(); self._paused.set(); self.engine.stop()
    @property
    def is_running(self): return self._paused.is_set() and not self._stopped.is_set()

# ===========================================================================
# GUI
# ===========================================================================
class SyncApp:
    def __init__(self,root):
        self.root=root; self.root.title("MySQL 通用增量同步工具 v3.0")
        self.root.geometry("860x570"); self.root.minsize(700,480)
        self.cfg=ConfigManager.load(); self.log_queue=queue.Queue()
        self.scheduler=None; self._manual_engine=None
        self._build_ui(); self._load_cfg_to_ui(); self._poll_log_queue()

    def _build_ui(self):
        # 控制栏固定在顶部
        cf=ttk.Frame(self.root); cf.pack(fill=tk.X,padx=4,pady=(4,2)); self._build_control_bar(cf)
        # 主区域
        mp=ttk.PanedWindow(self.root,orient=tk.VERTICAL); mp.pack(fill=tk.BOTH,expand=True,padx=4,pady=(0,4))
        nb=ttk.Notebook(mp); mp.add(nb,weight=1)
        for name,fn in [("数据库连接",self._build_conn_tab),("同步设置",self._build_sync_tab),("CDC状态",self._build_cdc_tab)]:
            f=ttk.Frame(nb); nb.add(f,text=name); fn(f)
        lf=ttk.LabelFrame(mp,text="运行日志"); mp.add(lf,weight=2); self._build_log_area(lf)
        # 状态栏
        sf=ttk.Frame(self.root,relief=tk.SUNKEN); sf.pack(fill=tk.X,side=tk.BOTTOM)
        self.status_var=tk.StringVar(value="就绪"); ttk.Label(sf,textvariable=self.status_var,padding=(8,2)).pack(side=tk.LEFT)
        self.phase_var=tk.StringVar(value=""); ttk.Label(sf,textvariable=self.phase_var,foreground="gray",padding=(0,2)).pack(side=tk.RIGHT,padx=8)

    def _build_conn_tab(self,p):
        # 可滚动画布（窗口小时滚动条自动出现，不用调整窗口）
        cv=tk.Canvas(p,highlightthickness=0); sb=ttk.Scrollbar(p,orient=tk.VERTICAL,command=cv.yview)
        inner=ttk.Frame(cv)
        inner.bind("<Configure>",lambda e:cv.configure(scrollregion=cv.bbox("all")))
        cw=cv.create_window((0,0),window=inner,anchor=tk.NW)
        cv.configure(yscrollcommand=sb.set)
        cv.pack(side=tk.LEFT,fill=tk.BOTH,expand=True); sb.pack(side=tk.RIGHT,fill=tk.Y)
        # 鼠标滚轮滚动
        def _wheel(e): cv.yview_scroll(int(-1*(e.delta/120)),"units")
        cv.bind("<Enter>",lambda e:cv.bind_all("<MouseWheel>",_wheel))
        cv.bind("<Leave>",lambda e:cv.unbind_all("<MouseWheel>"))
        # canvas 宽度变化时同步 inner 宽度
        cv.bind("<Configure>",lambda e:cv.itemconfig(cw,width=e.width))
        # 左右两栏
        l=ttk.LabelFrame(inner,text="源数据库 Source",padding=4); l.pack(side=tk.LEFT,fill=tk.BOTH,expand=True,padx=2,pady=2)
        r=ttk.LabelFrame(inner,text="目标数据库 Target",padding=4); r.pack(side=tk.RIGHT,fill=tk.BOTH,expand=True,padx=2,pady=2)
        fl=ttk.Frame(l); fl.pack(fill=tk.BOTH,expand=True); fr=ttk.Frame(r); fr.pack(fill=tk.BOTH,expand=True)
        self.src_entries=self._make_form(fl); self.tgt_entries=self._make_form(fr)
        ttk.Button(l,text="测试连接",command=lambda:self._test_conn("source")).pack(pady=(8,0))
        ttk.Button(r,text="测试连接",command=lambda:self._test_conn("target")).pack(pady=(8,0))
        ttk.Button(l,text="获取列名",command=lambda:self._fetch_cols("source")).pack(pady=(4,0))
        ttk.Button(r,text="获取列名",command=lambda:self._fetch_cols("target")).pack(pady=(4,0))

    def _make_form(self,p):
        e={}
        for i,(lb,k,df) in enumerate([("主机地址","host","127.0.0.1"),("端口","port","3306"),("用户名","user","root"),("密码","password",""),("数据库名","database",""),("表名","table","")]):
            ttk.Label(p,text=lb+":").grid(row=i,column=0,sticky=tk.W,pady=0)
            sh="*" if k=="password" else ""; v=tk.StringVar(value=df); e[k]=v
            ttk.Entry(p,textvariable=v,show=sh,width=22).grid(row=i,column=1,sticky=tk.EW,pady=0,padx=(4,0))
        p.columnconfigure(1,weight=1)
        return e

    def _build_sync_tab(self,p):
        f=ttk.Frame(p,padding=4); f.pack(fill=tk.BOTH,expand=True)
        r=[0]  # 用列表便于内部修改
        def nxt(): r[0]+=1; return r[0]-1
        # -- 模式选择 --
        r0=nxt()
        ttk.Label(f,text="同步模式:").grid(row=r0,column=0,sticky=tk.W,pady=1)
        mf=ttk.Frame(f); mf.grid(row=r0,column=1,sticky=tk.W,pady=1,padx=4)
        self.mode_var=tk.StringVar(value="pk")
        for v,t in [("pk","主键模式"),("append","追加模式"),("row_match","行匹配模式")]:
            ttk.Radiobutton(mf,text=t,variable=self.mode_var,value=v,command=self._on_mode_change).pack(side=tk.LEFT,padx=(0,8))
        # -- 变化检测字段 --
        row=nxt()
        ttk.Label(f,text="变化检测字段:").grid(row=row,column=0,sticky=tk.W,pady=1)
        self.change_field_var=tk.StringVar(value="updated_at")
        ttk.Entry(f,textvariable=self.change_field_var,width=22).grid(row=row,column=1,sticky=tk.W,pady=1,padx=4)
        # -- 主键 (pk模式) --
        row=nxt(); self.pk_row=row
        pk_lbl=ttk.Label(f,text="主键/唯一键:"); pk_lbl.grid(row=row,column=0,sticky=tk.W,pady=1)
        self.pk_var=tk.StringVar(value="id"); self.pk_entry=ttk.Entry(f,textvariable=self.pk_var,width=22); self.pk_entry.grid(row=row,column=1,sticky=tk.W,pady=1,padx=4)
        self._pk_widgets=[pk_lbl,self.pk_entry]
        # -- 匹配列 (row_match模式) --
        row=nxt(); self.mc_row=row
        mc_lbl=ttk.Label(f,text="匹配列(逗号分隔):"); mc_lbl.grid(row=row,column=0,sticky=tk.W,pady=1)
        self.match_cols_var=tk.StringVar(value=""); self.mc_entry=ttk.Entry(f,textvariable=self.match_cols_var,width=22); self.mc_entry.grid(row=row,column=1,sticky=tk.W,pady=1,padx=4)
        self._mc_widgets=[mc_lbl,self.mc_entry]
        # -- 调度间隔 --
        row=nxt()
        ttk.Label(f,text="调度间隔(秒):").grid(row=row,column=0,sticky=tk.W,pady=1)
        self.interval_var=tk.StringVar(value="300"); ttk.Entry(f,textvariable=self.interval_var,width=8).grid(row=row,column=1,sticky=tk.W,pady=1,padx=4)
        # -- 每批行数 --
        row=nxt()
        ttk.Label(f,text="每批行数:").grid(row=row,column=0,sticky=tk.W,pady=1)
        self.batch_size_var=tk.StringVar(value="500"); ttk.Entry(f,textvariable=self.batch_size_var,width=8).grid(row=row,column=1,sticky=tk.W,pady=1,padx=4)
        # -- Watermark --
        row=nxt()
        ttk.Label(f,text="Watermark延迟(秒):").grid(row=row,column=0,sticky=tk.W,pady=1)
        self.watermark_var=tk.StringVar(value="30"); ttk.Entry(f,textvariable=self.watermark_var,width=8).grid(row=row,column=1,sticky=tk.W,pady=1,padx=4)
        # -- Snapshot分片(pk模式) --
        row=nxt(); self.ch_row=row
        ch_lbl=ttk.Label(f,text="Snapshot分片行数:"); ch_lbl.grid(row=row,column=0,sticky=tk.W,pady=1)
        self.chunk_var=tk.StringVar(value="10000"); self.ch_entry=ttk.Entry(f,textvariable=self.chunk_var,width=8); self.ch_entry.grid(row=row,column=1,sticky=tk.W,pady=1,padx=4)
        self._ch_widgets=[ch_lbl,self.ch_entry]
        # -- 字段映射 --
        row=nxt()
        ttk.Label(f,text="字段映射(src:tgt):").grid(row=row,column=0,sticky=tk.NW,pady=1)
        mf2=ttk.Frame(f); mf2.grid(row=row,column=1,columnspan=2,sticky=tk.EW,pady=1,padx=4)
        self.mapping_text=tk.Text(mf2,height=4,width=40); ms2=ttk.Scrollbar(mf2,command=self.mapping_text.yview)
        self.mapping_text.configure(yscrollcommand=ms2.set); self.mapping_text.pack(side=tk.LEFT,fill=tk.BOTH,expand=True); ms2.pack(side=tk.RIGHT,fill=tk.Y)
        f.columnconfigure(1,weight=1)
        self._on_mode_change()

    def _on_mode_change(self):
        m=self.mode_var.get()
        # pk模式控件: 显示; 非pk: 隐藏
        pk_show = (m=="pk")
        for w in self._pk_widgets:
            (w.grid if pk_show else w.grid_remove)()
        for w in self._ch_widgets:
            (w.grid if pk_show else w.grid_remove)()
        # 匹配列控件: row_match模式显示
        mc_show = (m=="row_match")
        for w in self._mc_widgets:
            (w.grid if mc_show else w.grid_remove)()

    def _build_cdc_tab(self,p):
        f=ttk.Frame(p,padding=6); f.pack(fill=tk.BOTH,expand=True)
        ttk.Label(f,text="CDC 阶段:",font=("",10,"bold")).pack(anchor=tk.W)
        self.cdc_phase_label=ttk.Label(f,text="(未连接)",foreground="gray",font=("",10)); self.cdc_phase_label.pack(anchor=tk.W,pady=(2,8))
        ttk.Label(f,text="累计统计:",font=("",10,"bold")).pack(anchor=tk.W)
        sf=ttk.Frame(f); sf.pack(anchor=tk.W,pady=4)
        items=[("全量快照:","snap","0"),("增量同步:","incr","0"),("其中新增:","ins","0"),("其中覆盖:","upd","0")]
        self.cdc_stat_vars={}
        for i,(lb,k,df) in enumerate(items):
            ttk.Label(sf,text=lb).grid(row=i//2,column=(i%2)*2,sticky=tk.W,padx=(0,30))
            v=tk.StringVar(value=df); self.cdc_stat_vars[k]=v; ttk.Label(sf,textvariable=v).grid(row=i//2,column=(i%2)*2+1,sticky=tk.W)
        tk.Label(f,text="",height=2).pack()
        ttk.Label(f,text="操作:",font=("",10,"bold")).pack(anchor=tk.W,pady=(0,4))
        self.btn_reset=ttk.Button(f,text="重置Checkpoint(下次全量Snapshot)",command=self._reset_cdc_checkpoint); self.btn_reset.pack(anchor=tk.W,pady=2)
        self.btn_refresh=ttk.Button(f,text="刷新CDC状态",command=self._refresh_cdc_status); self.btn_refresh.pack(anchor=tk.W,pady=(8,0))

    def _build_control_bar(self,p):
        self.btn_save=ttk.Button(p,text="保存配置",command=self._save_config); self.btn_save.pack(side=tk.LEFT,padx=2)
        self.btn_sync_now=ttk.Button(p,text="立即同步",command=self._sync_now); self.btn_sync_now.pack(side=tk.LEFT,padx=2)
        self.btn_start=ttk.Button(p,text="启动定时",command=self._start_scheduler); self.btn_start.pack(side=tk.LEFT,padx=2)
        self.btn_pause=ttk.Button(p,text="暂停",command=self._pause_scheduler,state=tk.DISABLED); self.btn_pause.pack(side=tk.LEFT,padx=2)
        self.btn_stop=ttk.Button(p,text="停止",command=self._stop_all,state=tk.DISABLED); self.btn_stop.pack(side=tk.LEFT,padx=2)
        ttk.Separator(p,orient=tk.VERTICAL).pack(side=tk.LEFT,fill=tk.Y,padx=8)
        ttk.Button(p,text="清空日志",command=self._clear_log).pack(side=tk.LEFT,padx=2)
        ttk.Button(p,text="导出日志",command=self._export_log).pack(side=tk.LEFT,padx=2)

    def _build_log_area(self,p):
        tb=ttk.Frame(p); tb.pack(fill=tk.X,padx=4,pady=(2,0))
        ttk.Label(tb,text="级别:").pack(side=tk.LEFT); self.log_filter_var=tk.StringVar(value="ALL")
        for lb in ("ALL","INFO","WARN","ERROR"): ttk.Radiobutton(tb,text=lb,variable=self.log_filter_var,value=lb).pack(side=tk.LEFT,padx=3)
        self.log_text=scrolledtext.ScrolledText(p,wrap=tk.WORD,state=tk.DISABLED,font=("Consolas",9),bg="#1e1e1e",fg="#d4d4d4",insertbackground="white")
        self.log_text.pack(fill=tk.BOTH,expand=True,padx=4,pady=4)
        for t,c in [("INFO","#6a9955"),("WARN","#dcdcaa"),("ERROR","#f44747"),("TIME","#569cd6")]: self.log_text.tag_configure(t,foreground=c)

    # ==================================================================
    # 配置 <-> UI
    # ==================================================================
    def _load_cfg_to_ui(self):
        for side,ents in [("source",self.src_entries),("target",self.tgt_entries)]:
            for k,v in ents.items(): v.set(str(self.cfg[side].get(k,"")))
        sy=self.cfg["sync"]
        self.mode_var.set(sy.get("sync_mode","pk"))
        self.change_field_var.set(sy.get("change_field","updated_at"))
        self.pk_var.set(sy.get("primary_keys","id"))
        self.match_cols_var.set(sy.get("match_columns",""))
        self.interval_var.set(str(sy.get("interval_seconds",300)))
        self.batch_size_var.set(str(sy.get("batch_size",500)))
        self.watermark_var.set(str(sy.get("watermark_delay_seconds",30)))
        self.chunk_var.set(str(sy.get("snapshot_chunk_size",10000)))
        m=self.cfg.get("field_mapping",{})
        ls=[f"{k}:{v}" for k,v in m.items()] if m else []
        self.mapping_text.delete("1.0",tk.END); self.mapping_text.insert("1.0","\n".join(ls))
        self._on_mode_change()

    def _read_ui_to_cfg(self):
        for side,ents in [("source",self.src_entries),("target",self.tgt_entries)]:
            for k,v in ents.items():
                val=v.get(); self.cfg[side][k]=int(val) if k=="port" and val.isdigit() else val
        sy=self.cfg["sync"]
        sy["sync_mode"]=self.mode_var.get()
        sy["change_field"]=self.change_field_var.get()
        sy["primary_keys"]=self.pk_var.get()
        sy["match_columns"]=self.match_cols_var.get()
        sy["interval_seconds"]=int(self.interval_var.get() or "300")
        sy["batch_size"]=int(self.batch_size_var.get() or "500")
        sy["watermark_delay_seconds"]=int(self.watermark_var.get() or "30")
        sy["snapshot_chunk_size"]=int(self.chunk_var.get() or "10000")
        raw=self.mapping_text.get("1.0",tk.END).strip(); m={}
        if raw:
            for ln in raw.split("\n"):
                ln=ln.strip()
                if ":" in ln: k,v=ln.split(":",1); m[k.strip()]=v.strip()
        self.cfg["field_mapping"]=m

    # ==================================================================
    # 动作
    # ==================================================================
    def _save_config(self): self._read_ui_to_cfg(); ConfigManager.save(self.cfg); self._set_status("已保存")
    def _test_conn(self,side):
        e=self.src_entries if side=="source" else self.tgt_entries
        info={k:int(v.get()) if k=="port" and v.get().isdigit() else v.get() for k,v in e.items()}
        ok,msg=DBHelper.test(info)
        (messagebox.showinfo if ok else messagebox.showerror)("连接测试",f"{side} {'连接成功' if ok else '连接失败:\n'+msg}")
    def _fetch_cols(self,side):
        e=self.src_entries if side=="source" else self.tgt_entries; db=e["database"].get(); tb=e["table"].get()
        if not db or not tb: messagebox.showwarning("提示","请先填数据库名和表名"); return
        info={k:int(v.get()) if k=="port" and v.get().isdigit() else v.get() for k,v in e.items()}
        try:
            cs=DBHelper.get_columns(info,db,tb); messagebox.showinfo(f"{side}表列名({len(cs)}列)","\n".join(cs))
        except Exception as ex: messagebox.showerror("获取失败",str(ex))
    def _sync_now(self):
        self._read_ui_to_cfg(); self._set_status("手动同步中...")
        self.btn_sync_now.configure(state=tk.DISABLED); self.btn_stop.configure(state=tk.NORMAL,command=self._stop_manual)
        threading.Thread(target=self._run_sync_thread,daemon=True).start()
    def _run_sync_thread(self):
        self._manual_engine=SyncEngine(self.cfg,self.log_queue); self._manual_engine.run_once(); self._manual_engine=None
        self.root.after(0,self._on_manual_sync_done)
    def _on_manual_sync_done(self):
        self._set_status("同步完成"); self._refresh_cdc_status()
        self.btn_sync_now.configure(state=tk.NORMAL); self.btn_stop.configure(state=tk.DISABLED,command=self._stop_all)
    def _stop_manual(self):
        if self._manual_engine: self._manual_engine.stop(); self._manual_engine=None
        self._set_status("已停止"); self.btn_sync_now.configure(state=tk.NORMAL); self.btn_stop.configure(state=tk.DISABLED,command=self._stop_all)
    def _start_scheduler(self):
        self._read_ui_to_cfg()
        if self.scheduler and self.scheduler.is_alive(): return
        self.scheduler=SchedulerThread(self.cfg,self.log_queue); self.scheduler._paused.set(); self.scheduler.start()
        self._set_status("定时同步运行中"); self.btn_start.configure(state=tk.DISABLED); self.btn_pause.configure(state=tk.NORMAL); self.btn_stop.configure(state=tk.NORMAL,command=self._stop_all)
    def _pause_scheduler(self):
        if self.scheduler and self.scheduler.is_running: self.scheduler.pause(); self.btn_pause.configure(text="继续",command=self._resume_scheduler); self._set_status("已暂停")
    def _resume_scheduler(self):
        if self.scheduler: self.scheduler.resume(); self.btn_pause.configure(text="暂停",command=self._pause_scheduler); self._set_status("运行中")
    def _stop_all(self):
        if self._manual_engine: self._manual_engine.stop(); self._manual_engine=None
        if self.scheduler: self.scheduler.stop(); self.scheduler=None
        self.btn_start.configure(state=tk.NORMAL); self.btn_sync_now.configure(state=tk.NORMAL)
        self.btn_pause.configure(state=tk.DISABLED,text="暂停",command=self._pause_scheduler); self.btn_stop.configure(state=tk.DISABLED,command=self._stop_all)
        self._set_status("已停止")
    def _clear_log(self): self.log_text.configure(state=tk.NORMAL); self.log_text.delete("1.0",tk.END); self.log_text.configure(state=tk.DISABLED)
    def _export_log(self):
        from tkinter import filedialog
        p=filedialog.asksaveasfilename(defaultextension=".log",filetypes=[("Log files","*.log")])
        if not p: return
        with open(p,"w",encoding="utf-8") as f: f.write(self.log_text.get("1.0",tk.END))
        self._set_status(f"已导出:{p}")
    def _reset_cdc_checkpoint(self):
        if not messagebox.askyesno("确认","重置后下次运行将重新全量同步。\n\npk模式:ON DUPLICATE KEY UPDATE去重\nappend/row_match:先清空目标表再全量拷贝\n\n确定?"): return
        self._read_ui_to_cfg()
        e=SyncEngine(self.cfg,self.log_queue); e._reset_checkpoint()
        self._set_status("已重置"); self._refresh_cdc_status()
    def _refresh_cdc_status(self):
        self._read_ui_to_cfg()
        try:
            e=SyncEngine(self.cfg,self.log_queue); cp=e._load_checkpoint()
            pt={"snapshot":"Snapshot全量快照","incremental":"Incremental增量同步"}
            self.cdc_phase_label.configure(text=pt.get(cp["phase"],cp["phase"])); self.phase_var.set(f"Phase:{cp['phase']}")
            self.cdc_stat_vars["snap"].set(str(cp["snap"])); self.cdc_stat_vars["incr"].set(str(cp["incr"]))
            self.cdc_stat_vars["ins"].set(str(cp["ins"])); self.cdc_stat_vars["upd"].set(str(cp["upd"]))
        except Exception as e: self.cdc_phase_label.configure(text=f"(无法连接:{e})",foreground="red")

    # ==================================================================
    # 日志
    # ==================================================================
    def _poll_log_queue(self):
        fl=self.log_filter_var.get()
        try:
            while True:
                lv,msg=self.log_queue.get_nowait()
                if fl!="ALL" and lv!=fl: continue
                self._append_log(lv,msg)
        except queue.Empty: pass
        self.root.after(200,self._poll_log_queue)
    def _append_log(self,lv,msg):
        ts=datetime.now().strftime("%H:%M:%S"); self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END,ts+" ","TIME"); self.log_text.insert(tk.END,f"[{lv:<5}] ",lv); self.log_text.insert(tk.END,msg+"\n")
        self.log_text.see(tk.END); self.log_text.configure(state=tk.DISABLED)
    def _set_status(self,t): self.status_var.set(t)

# ===========================================================================
# 入口
# ===========================================================================
def main():
    r=tk.Tk(); app=SyncApp(r)
    def on_close():
        if app.scheduler and app.scheduler.is_alive():
            if messagebox.askyesno("确认","定时同步运行中,确定退出?"): app.scheduler.stop()
            else: return
        r.destroy()
    r.protocol("WM_DELETE_WINDOW",on_close); app._refresh_cdc_status(); r.mainloop()

if __name__=="__main__": main()
