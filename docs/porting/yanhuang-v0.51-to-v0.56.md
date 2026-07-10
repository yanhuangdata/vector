# 移植报告:yanhuang 自定义改动 v0.51 → v0.56

## 1. 背景与设计决策

### 1.1 分支拓扑(为什么不能直接反向合并)

upstream 的版本分支**不是线性递进**的。`vector/v0.51` 与 `vector/v0.56`
都从公共提交 `aef66cfae8`(`feat(codecs): bump avro-rs #24119`)各自独立分叉演进:

```
                 ┌─→ v0.51 ──→ (yanhuang 在此之上加自定义提交) = origin/yanhuang/v0.51
aef66cfae8 ──────┤
                 └─→ v0.52 → v0.53 → v0.54 → v0.55 → v0.56
```

验证:`git merge-base --is-ancestor vector/v0.51 vector/v0.56` 返回 **NO**。

因此若把 `vector/v0.56` 合并进 `yanhuang/v0.51`,三方合并的 base 会退到
`aef66cfae8`,upstream 自身 0.52→0.56 的全部演进(测试配置、vdev、website 等)
都会变成冲突 —— 实测产生 **358 个冲突文件**,绝大多数是噪音。

### 1.2 采用的方案

**以 `vector/v0.56` 为基底,把 yanhuang 的纯自定义 diff 移植上去。**

- 新建分支:`yanhuang/v0.56`(从 `vector/v0.56`)
- 提取自定义补丁:`git diff vector/v0.51..origin/yanhuang/v0.51`
  (因为 `vector/v0.51` 确是 `yanhuang/v0.51` 的祖先,这段 diff 就是纯粹的自研改动)
- 排除自动生成 / 需重算的文件:`Cargo.lock`、`*/yarn.lock`、
  以及 v0.56 中已被移动的 `src/source_sender/mod.rs`(改动仅为一个空白行)
- `git apply --3way` 应用,逐个解决冲突,最后 `cargo check` 验证

### 1.3 自定义功能足迹

yanhuang 相对纯净 v0.51 只改了 **25 个文件**,核心自研功能:

| 模块 | 功能 |
|------|------|
| `file-source-common/fingerprinter` | 新增指纹策略:`ModificationTime`、`FullContentChecksum`、`ChecksumWithPathSalt`(sha2 加盐);保留旧实现为 `fingerprinter_old.rs` |
| `file-source/file_watcher` | EOF linger line 读取、`trigger_wait_sec` 触发等待、`is_sleep`/`data_ready_time` 状态 |
| `sources/exec` | cron job 类型的 exec source + `CronJobError` 内部事件 |
| `vector-stream/driver` | 背压 / 在途请求数据大小限制、缓存收缩 |
| `sources/file`、`kubernetes_logs` | 透传上述 file 配置项 |

---

## 2. 冲突文件与合并处理详解

`git apply --3way` 后共 **3 个文件冲突**,另有 **2 处编译错误**因 v0.56 API 演进产生。

### 2.1 `lib/file-source/src/file_watcher/mod.rs`(3 处冲突,核心难点)

v0.56 upstream 给 `FileWatcher` 引入了 **EOF 退避机制**
(`read_retry_delay` 字段 + `track_read_eof()`,读到 EOF 后指数退避
`1ms → 250ms`,避免空轮询)。yanhuang 则在同一区域加了 linger line 读取。

**冲突 1 —— 结构体字段(第 64 行)**
两边各加了独立字段,互不冲突,**两者都保留**:

```rust
read_retry_delay: Duration,   // upstream v0.56:EOF 退避延迟
data_ready_time: Instant,     // yanhuang:配合 trigger_wait_sec 的数据就绪时间
```

**冲突 2 —— 结构体初始化(第 190 行)**
同上,两个字段初始值都保留:

```rust
read_retry_delay: EOF_READ_BACKOFF_MIN,
data_ready_time: Instant::now(),
```

**冲突 3 —— EOF 读取分支(真正需要动脑的一处)**

- upstream v0.56 版本:读到 EOF 时调用 `self.track_read_eof()`(触发退避)
- yanhuang v0.51 版本:读到 EOF 时 `self.reached_eof = true`,若开启
  linger line 且缓冲区非空,则把缓冲区剩余内容作为一行吐出

处理方式:**先调用 upstream 的 `track_read_eof()`,再套用 yanhuang 的
linger 分支**。关键点是 `track_read_eof()` 内部已经会设置
`self.reached_eof = true`,所以 yanhuang 原来那行手写的 `reached_eof = true`
被删除,避免重复:

```rust
} else {
    self.track_read_eof();                 // v0.56:设置退避 + reached_eof=true
    if self.read_eof_linger_line && !self.buf.is_empty() {
        // eof linger line read flag is set, get the linger line.
        Ok(RawLineResult {
            raw_line: Some(RawLine {
                offset: initial_position,
                bytes: self.buf.split().freeze(),
            }),
            discarded_for_size_and_truncated,
        })
    } else {
        Ok(RawLineResult { raw_line: None, discarded_for_size_and_truncated })
    }
}
```

两个功能**语义正交、可安全共存**:`track_read_eof()` 只影响下一次
`should_read()` 的调度节奏,不改变本次返回内容;linger 分支只决定本次
是否吐出残余行。`reset`/`roll_over` 时 upstream 会把
`read_retry_delay` 重置回 `EOF_READ_BACKOFF_MIN`,不影响 linger 逻辑。

### 2.2 `src/internal_events/exec.rs`(1 处冲突 + 1 处 API 修复)

**冲突 —— derive 属性(第 43 行)**
v0.56 给所有内部事件统一加了 `NamedInternalEvent` derive 宏。yanhuang 在
`ExecFailedError` 前面插入了 `CronJobError` 结构体。合并后:
`CronJobError` 也套用新 derive,`ExecFailedError` 恢复 v0.56 的 derive:

```rust
#[derive(Debug, NamedInternalEvent)]
pub struct CronJobError<'a> { ... }      // yanhuang 自研,补上新 derive

#[derive(Debug, NamedInternalEvent)]
pub struct ExecFailedError<'a> { ... }   // upstream,恢复 derive
```

**编译修复 —— `counter!` API 变更(E0308)**
v0.56 把 `counter!` 第一个参数从字符串字面量改成了 `CounterName` 枚举。
yanhuang 的 `CronJobError` 仍用旧的字符串写法,编译报
`expected CounterName, found &str`。因
`CounterName::ComponentErrorsTotal` 恰好映射为 `"component_errors_total"`
(见 `metric_name.rs:270`),语义完全等价,故改为枚举:

```rust
counter!(
    CounterName::ComponentErrorsTotal,   // 原为 "component_errors_total"
    "schedule" => self.schedule.to_owned(),
    "error_type" => error_type::CONFIGURATION_FAILED,
    "stage" => error_stage::RECEIVING,
).increment(1);
```

### 2.3 `website/assets/js/search.tsx`(直接取 upstream)

排查发现 **yanhuang 分支里这个文件本身是损坏的合并残留**:
重复的 `import`(第 1-2 行与 3-4 行完全一样)、重复的 `const Result`
声明 —— 这段代码根本无法通过编译,是历史上一次 botched merge 的产物,
而非真实功能。且 Typesense 搜索本就是 upstream v0.51/v0.56 已有的功能。

处理方式:**直接采用 v0.56 干净版本**
(`git checkout vector/v0.56 -- website/assets/js/search.tsx`),
不移植这段损坏 diff。

### 2.4 `lib/file-source/src/file_watcher/tests/mod.rs`(编译修复,E0063)

v0.56 新增的单测 `watcher_for_timing()` 直接构造 `FileWatcher{...}`,
但缺少 yanhuang 新增的 4 个字段,报 "missing fields"。补齐:

```rust
is_sleep: false,
data_ready_time: now,
read_eof_linger_line: false,
trigger_wait_sec: None,
```

### 2.5 `Cargo.lock` / 依赖

`git apply --3way` 自动合入两个新依赖,无需人工干预:
- `cron = "0.13.0"`(exec cron job)
- `sha2 = "0.10.8"`(指纹加盐 checksum)

---

## 3. 合并后的功能影响

### 3.1 新增/保留能力(功能层面无回退)

移植后 yanhuang 的全部自研功能均完整保留,且叠加了 v0.56 的改进:

- **file_watcher 同时具备**:v0.56 的 EOF 指数退避(降低空文件轮询 CPU)
  **+** yanhuang 的 linger line 读取和 `trigger_wait_sec` 触发等待。
  二者正交,不互相削弱。
- **fingerprinter**:三种自研指纹策略(含 sha2 加盐)完整保留。
- **exec cron source** 与 `CronJobError` 事件完整保留,事件命名/计数器
  已适配 v0.56 的 `NamedInternalEvent` + `CounterName` 规范。
- **vector-stream driver** 背压限制完整保留。

### 3.2 行为变化点(需回归关注)

- **file_watcher EOF 调度节奏改变**:引入 v0.56 退避后,EOF 状态下的读取
  重试从"固定节奏"变为"指数退避(最长 250ms)"。对 linger line 的**内容**
  无影响,但 linger line 被吐出的**时机**可能比 v0.51 略有延迟(受退避影响)。
  建议对"文件写入后不换行、等待 linger 输出"的场景做一次回归。
- **`CronJobError` 指标名称不变**:枚举 `ComponentErrorsTotal` 映射字符串
  仍是 `component_errors_total`,下游 dashboard/告警**无需改动**。
- **website 搜索**:采用 upstream 版本,行为与官方 v0.56 一致(原 yanhuang
  版本本就无法编译,不存在功能损失)。

### 3.3 验证状态

- ✅ `cargo check --workspace --all-targets` 通过(exit 0,1m22s)
- ⬜ 尚未运行 `make check-clippy`
- ⬜ 尚未运行单元测试(建议至少跑 file-source / exec 相关)
- ⬜ 尚未做运行时回归(尤其 EOF linger line 时机)
- ⬜ 改动尚未提交

### 3.4 遗留事项

- `src/source_sender/mod.rs` 在 v0.56 已移至
  `lib/vector-core/src/source_sender/mod.rs`;yanhuang 对该文件的改动仅是
  一个空白行,已忽略,无功能影响。
- 若后续还需从 v0.51 之后的 yanhuang 提交里带其他改动,应以同样方式
  (提取纯 diff → 打到 v0.56)处理,不要反向合并版本分支。
