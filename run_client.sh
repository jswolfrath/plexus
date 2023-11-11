#!/usr/bin/env bash

rm -rf /tmp/join/*
rm -rf /media/join/*

rm s*.log

make client  > s0.log &
make client1 > s1.log &
