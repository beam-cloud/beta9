#!/usr/bin/env bash

if ! command -v vhs >/dev/null 2>&1; then
  echo "VHS command not found"
  echo "Check out how to install it at https://github.com/charmbracelet/vhs"
  exit 1
fi

cd ../examples/demo
PS1='$ ' vhs ../../bin/demo.tape
