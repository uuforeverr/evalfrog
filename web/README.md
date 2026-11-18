# EvalFrog Web

Human Web Canvas is an independent static artifact introduced in M10. It only
calls the versioned External API through `/api`; it never uploads DSL or Source
Map, never owns runtime state, and edits the same Canonical IR as Agent CLI.

`npm run build` copies the dependency-free static artifact to `web/dist`.
`npm test` verifies the UI stays on the public authoring/runtime surface.
`npm run test:browser` runs the Draft conflict selection and cancellation flow against a local mock API using Playwright Chromium.

使用时先填写 Project、Workflow、Token 与 API Base URL，再点击“加载 Draft”。每个节点左右两侧显示输入/输出连接点：从右侧圆点拖线到另一节点左侧圆点即可新增控制边；点击节点旁的“＋”可选择 Catalog 类型，插入指定出边或仅放置节点后手动拉线。点击节点在右侧设置标题、输入和输出，点击“应用节点设置”后再保存 Draft：Literal 可直接填写 JSON，支持 Ref 的输入可选择类型兼容且在控制图上可到达的前序命名输出；固定输出只读，Code 输出可声明。Branch 的 `value` 可引用前序输出，条件和默认分支在节点面板编辑；创建 Branch 不要求填写 route，插入原有连线时后续路径默认接到 Default Route。新 Branch 连线可先不指定条件，再点击边上标签选择其 Case 或 Default Route；未指定条件的 Draft 可保存，但校验或发布会拒绝。可到达不代表每条激活路径都安全，条件分支汇合后的引用由服务端继续检查。节点和边都可右键删除，或选中后按 Backspace/Delete 删除。输入框与 JSON 编辑器获得焦点时，这些快捷键不会删除画布元素。拖拽节点实时更新位置与连接，松开后写回 IR。删除节点会列出受影响的边和数据引用，经确认后移除。左侧“高级画布操作”仍提供选择器方式，右侧“高级：直接编辑 IR JSON”保留完整配置编辑入口。所有保存、校验、测试、发布、正式运行和取消都走同一 External API；新增节点的示例配置仍应通过“校验”确认。运行进度通过 SSE wake-up 触发重新读取 Run Read Model，不以浏览器本地状态作为权威。

顶部状态栏显示 Draft 加载、保存中、保存成功（含最新 Revision）、保存失败及并发冲突；失败详情仍可在诊断区域查看。

三方合并按内容比较节点，忽略 JSON 对象字段顺序；同一节点的输入、输出或其他参数变化需要按整个节点选择。边按起点、终点和 Branch route 比较，节点参数变化不会让未改连接重复待选。完全未变的节点和边自动保留；仅一方移动的布局自动采用，双方移动到不同位置才要求选择。

合并画布中的节点可直接拖动，连线随之更新；手动位置随最终选择写入 Draft，删除节点时不保留其布局。

保存遇到 Draft Revision 冲突时，Web 保留本地 IR，并在合并画布上展示共同基线、已保存版本和本地提交的差异。任何一方修改过的整个节点、边及布局项都需要逐项选择；确认后以当前 Revision 和新幂等键重新保存。如果期间又发生保存，已选结果作为本地版本进入下一轮选择。

新建工作流会使用 IR v2。Start 面板中的输出声明就是调用输入参数；左侧“调用输入”填入每个参数的 JSON 值。End 面板为每个具名返回值填写 Literal 或选择前序输出，输出声明随输入同步。Branch v2 可在一个 IF/ELSE IF 分支内添加多个 AND/OR 判断，未命中时走 ELSE。已有 IR v1 工作流仍按原有固定 Start/End/Branch 契约编辑和运行；“升级 v1 Draft”会在本地转换旧条件并提示调用及返回对象形状变化，作者复核后再保存。HTTP/RPC 保留固定的 `response` 输出；自定义输出字段使用 Code 节点处理响应。
