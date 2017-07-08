#!/bin/bash
cd events/ ; GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-w" -o eventer ; cd ../ ; mv events/eventer ./
cd metrics/ ; GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-w" -o heapster ; cd ../ ; mv metrics/heapster ./
