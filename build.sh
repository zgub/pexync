#!/bin/bash

go build -o PeXync -ldflags "-X github.com/zgub/pexync/cmd.Version=`git tag --sort=-version:refname | head -n 1`"