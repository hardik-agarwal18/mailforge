package smtp

import (
	"fmt"
	"strings"
)

type Command struct {
	Name string
	Arg  string
}

func ParseCommand(line string) (Command, error) {
	line = strings.TrimSpace(line)

	if line == "" {
		return Command{}, fmt.Errorf("empty command")
	}

	parts := strings.SplitN(line, " ", 2)

	command := Command{
		Name: strings.ToUpper(parts[0]),
	}

	if len(parts) == 2 {
		command.Arg = strings.TrimSpace(parts[1])
	}

	return command, nil
}
