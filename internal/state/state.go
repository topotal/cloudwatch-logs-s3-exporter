package state

type States []*State

type State struct {
	LogGroupArn  string `json:"LogGroupArn"`
	LogStreamArn string `json:"LogStreamArn"`
	ExportedAt   int64  `json:"ExportedAt"`
	TaskId       string `json:"TaskId"`
}
