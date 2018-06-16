CHISTRIBUTED_VERSION := $(shell chistributed --version 2>/dev/null)

start: check kill
	chistributed --run start.chi --debug --verbose

check:
ifndef CHISTRIBUTED_VERSION
	@echo "chistributed not found. Did you forget to activate a virtualenv?"
	exit 1
else
	@echo "chistributed found, starting"
endif

kill:
	-pkill -9 chistributed
	-pkill -9 python
	-pkill -9 make
