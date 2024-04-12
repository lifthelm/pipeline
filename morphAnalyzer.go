package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

type Term struct {
	NormalForm, Tag string
}

func executePythonScript(word string) (string, error) {
	cmd := exec.Command("./venv/bin/python3", "parse_word.py", word)
	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("error executing Python script: %s", err)
	}

	return out.String(), nil
}

func ParseWord(word string) (Term, error) {
	outString, err := executePythonScript(word)
	if err != nil {
		return Term{}, err
	}
	words := strings.Fields(outString)
	return Term{
		NormalForm: words[0],
		Tag:        words[1],
	}, nil
}
