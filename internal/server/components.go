package server

import (
	"fmt"

	v1 "github.com/dubi-project/dubi/api/v1"
	stateengine "github.com/dubi-project/dubi/internal/engine/state"
	configmodule "github.com/dubi-project/dubi/internal/module/config"
)

// Called only after State and Config have been successfully constructed for
// this public entry. Planned engines/modules are intentionally not registered.
func publicComponentCatalog(nodeID string) v1.ComponentCatalog {
	return v1.ComponentCatalog{NodeID: nodeID, Scope: "entry_node", Items: []v1.RegisteredComponent{
		{ID: stateengine.EngineID, Kind: "engine", Name: "State", Subtitle: "状态引擎", Description: "保存键值状态与版本，为业务模块提供状态读写和条件更新能力。", Status: "registered",
			Capabilities: []string{"键值读写", "版本控制", "条件更新"}, Engines: []string{}, Settings: []v1.ComponentSetting{
				{Label: "单值上限", Value: fmt.Sprintf("%d KiB", stateengine.MaxValueBytes/1024)},
				{Label: "Key 上限", Value: fmt.Sprintf("%d KiB", stateengine.MaxKeyBytes/1024)},
				{Label: "设置方式", Value: "内置限制"},
			}},
		{ID: configmodule.ModuleID, Kind: "module", Name: "配置中心", Subtitle: "Config", Description: "管理租户的配置数据，支持配置读取、写入、删除和版本条件更新。", Status: "registered",
			Capabilities: []string{"配置读写", "条件更新", "租户隔离"}, Engines: []string{stateengine.EngineID}, Settings: []v1.ComponentSetting{
				{Label: "单项大小上限", Value: fmt.Sprintf("%d KiB（由 State 限制）", stateengine.MaxValueBytes/1024)},
				{Label: "访问方式", Value: "HTTP 业务 API"},
			}},
	}}
}
