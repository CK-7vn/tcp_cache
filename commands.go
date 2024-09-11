package main

import (
	"errors"
	"fmt"

	//"strconv"
	"strings"
	"time"
)

type Command string

const (
	CMDSet    Command = "SET"
	CMDGet    Command = "GET"
	CMDDelete Command = "DELETE"
)

type Msg struct {
	Cmd   Command
	Key   []byte
	Value []byte
	TTL   time.Duration
}

func ParseMSG(raw []byte) (*Msg, error) {
	var (
		rawStr = strings.TrimSpace(string(raw))
		parts  = strings.Split(rawStr, " ")
	)
	if len(parts) < 2 {
		return nil, errors.New("Invalid protocol format less than 2 parts\n")
	}
	msg := &Msg{
		Cmd: Command(strings.ToUpper(parts[0])),
		Key: []byte(parts[1]),
	}

	if msg.Cmd == CMDSet {
		if len(parts) < 4 {
			return nil, errors.New("Invalid set command\n")
		}
		msg.Value = []byte(parts[2])
		ttlStr := parts[3]

		expirationTime, err := time.Parse(time.RFC3339, ttlStr)
		if err == nil {
			now := time.Now().UTC()
			if expirationTime.Before(now) {
				return nil, errors.New("Expiration time is in the past\n")
			}
			msg.TTL = expirationTime.Sub(now)
		} else {
			duration, err := time.ParseDuration(ttlStr)
			if err != nil {
				return nil, fmt.Errorf("invalid TTL format: %v\n", err)
			}
			msg.TTL = duration
		}
	}
	return msg, nil
}

// CMDDELETE TODO
func (msg *Msg) ToBytes() []byte {
	switch msg.Cmd {
	case CMDSet:
		cmd := fmt.Sprintf("%s %s %s %s", msg.Cmd, msg.Key, msg.Value, msg.TTL)
		return []byte(cmd)
	case CMDGet:
		cmd := fmt.Sprintf("%s %s ", msg.Cmd, msg.Key)
		return []byte(cmd)
	case CMDDelete:
		cmd := fmt.Sprintf("%s %s", msg.Cmd, msg.Key)
		return []byte(cmd)
	default:
		panic("Unknown command\n")
	}
}
