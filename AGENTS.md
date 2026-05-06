---
alwaysApply: true
description: 项目规范 (Project Rules)
---
# 项目规范 (Project Rules)

本文档定义了项目的开发规范，旨在确保代码的一致性和可维护性。AI 生成代码时必须严格遵守此规范。

# 项目介绍
本项目是基于 https://github.com/coder/websocket 的websocket 上层封装库

## 最重要的前提
- 思考代码的可维护性、可扩展性、可复用性。
- 避免重复代码，保持代码的 DRY (Don't Repeat Yourself) 原则。
- 如果能抽象出来一个工具或者组件的，就抽象出来一个工具或者组件。
- 严禁写出代码屎山，即代码重复、逻辑复杂、可维护性差的代码。
- 不用过度设计， 大道至简。
- 注重代码性能，可用零拷贝就用，可用池化就用

## 1. 技术栈概览
- **语言**: Go 1.26+
- **依赖注入**: Google Wire
- **日志库**: Slog (Structured Logging)

## 2. 编码规范
- **日志**: 使用 `log/slog` 进行结构化日志记录，通过依赖注入传入 Logger。
- **依赖注入**: 使用 `google/wire` 进行依赖注入。修改依赖关系后需重新生成 `wire_gen.go`

## 3. 注意事项
- 文件命名保持一致，避免拼写错误。

