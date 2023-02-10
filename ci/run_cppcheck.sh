#!/bin/sh

cppcheck --enable=style --inconclusive --suppress=unusedFunction --suppress=unreadVariable --suppress=accessMoved src/
