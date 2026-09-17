# VLink 二开 Core 的本地构建

Core 是移动端 SDK，不是 OpenIM 服务端。RN fork 提供桥接，VLink App 消费桥接和本页产物。三个仓库必须分别保留提交；只合并 App 不会自动携带 Core 源码。

## 工具与输入

- 当前验证工具：Go 1.24.0、Python 3.9+、Xcode（iOS）、JDK 17、Android SDK/NDK 27.1.12297006。
- gomobile / gobind 固定为 `golang.org/x/mobile@v0.0.0-20250520180527-a1d90793fc63`，与 go.mod 一致。
- 设置 `PATH`、`ANDROID_HOME`、`ANDROID_NDK_HOME`、`JAVA_HOME` 到实际机器目录；脚本不写死开发者路径。需要时用 `GOMOBILE` 指定可执行文件。
- 输出目录必须尚不存在；脚本拒绝脏工作区，只从当前 Git 提交生成源码快照。保留有本地日志的开发目录时，可另建干净的 detached worktree 构建，不必删除现场。

```sh
go install golang.org/x/mobile/cmd/gomobile@v0.0.0-20250520180527-a1d90793fc63
go install golang.org/x/mobile/cmd/gobind@v0.0.0-20250520180527-a1d90793fc63
gomobile init
python3 scripts/build-local-core.py --output /absolute/path/to/core-output --platform all
```

`--prepare-only` 只验证源码导出、摘要和依赖布局，不调用原生编译，不能算双端构建通过。单平台可用 `--platform android` 或 `--platform ios`，输出目录仍须独立；App 双端集成通常使用 `all`。

## 产物契约

- `build-manifest.json`：Core 提交、源码摘要、Android 本地 Maven 版本。
- `source-input.json` / `source/`：本次提交快照及逐文件摘要。
- Android：`openim-core.aar` 和 `maven/io/openim/core-sdk/<version>/`。
- iOS：`OpenIMCore.xcframework`、`OpenIMSDKCore.podspec`、`LICENSE`。
- 每个平台独立输出构建结果 JSON 和日志。只有 exitCode=0 的平台才可交付；manifest 存在本身不表示编译成功。

App 使用 `OPENIM_CORE_LOCAL_DIR=/absolute/path/to/core-output` 指向此目录，重新执行 Pod 安装及 Android 原生构建。Core 修改不能靠 JS 热更新生效。脚本保留既有双端 bind 参数；Android Makefile 的独立目标不等于本脚本的构建基线。

## 继续开发与合并

1. 从保留的 Core 提交建立新分支/工作树，修改和提交 Core。
2. 按本页重建产物；App 更新依赖路径并记录 manifest。不要覆盖旧产物目录，便于回溯。
3. RN 公共桥接有改动时，单独提交 RN fork，再重新安装 App 本地依赖。
4. App main 保留自有 Core 的 Pod/Gradle 接入，不能为恢复包名而回退到官方旧 Core。
5. 当前不推送远程。其他机器无法仅从远程获得这些本地提交，应使用已验证的 Git bundle，或待用户授权后推送。

对应 App 的完整入口与提测边界见 `app/docs/sync-v2-reference/README.md`。
