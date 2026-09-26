package driver

import "errors"

// 管理器可能返回的错误，调用方可通过 errors.Is 区分。
var (
	// ErrDeviceNotFound 未注册指定设备。
	ErrDeviceNotFound = errors.New("driver: device not found")
	// ErrDeviceUnavailable 设备已隔离，暂不提供数据。
	ErrDeviceUnavailable = errors.New("driver: device unavailable (isolated)")
	// ErrAlreadyStarted 管理器已启动。
	ErrAlreadyStarted = errors.New("driver: already started")
	// ErrNotStarted 管理器尚未启动。
	ErrNotStarted = errors.New("driver: not started")
	// ErrStaleReading 设备降级中，最新读数已超过时效窗口。
	ErrStaleReading = errors.New("driver: latest reading is stale")
	// ErrDuplicateDevice 重复注册同一设备 ID。
	ErrDuplicateDevice = errors.New("driver: duplicate device id")
)
