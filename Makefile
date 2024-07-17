#!/usr/bin/env make

SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: udemy_exerc1
udemy_exerc1:
	python3 src/udemy/exerc1.py

.PHONY: udemy_exerc2
udemy_exerc2:
	python3 src/udemy/exerc2.py

.PHONY: udemy_exerc3
udemy_exerc3:
	python3 src/udemy/exerc3.py
