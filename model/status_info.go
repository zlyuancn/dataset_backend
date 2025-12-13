package model

const (
	Op_Biz    = "biz"    // 业务操作
	Op_System = "system" // 系统/平台逻辑操作
)

// 操作信息
const (
	StatusInfo_UserOp           = "user op"             // 用户操作
	StatusInfo_UserCreateAndRun = "user create and run" // 创建并启动
	StatusInfo_UserChangeStatus = "user change status"  // 仅变更状态
	StatusInfo_RestorerProcess  = "restorer process"    // 恢复处理
)
