# -navicat---
 针对Navicat无法增量迁移、失败难续传的问题，开发了一款支持主键去重（UPSERT）、纯追加、行匹配（DELETE+INSERT）三种模式的同步工具。通过变化字段 + Watermark捕获增量，分片批量读取提升全量效率，本地Checkpoint实现断点续传。内置Tkinter可视化面板，支持定时调度、实时日志和 CDC 状态监控。在开发中利用ai工具辅助生成基础代码。
