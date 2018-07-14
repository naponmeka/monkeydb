fcuk:
	echo "FCUK"

clean:
	rm -rf tmp monkeydb nohup.out

build:
	go build -o monkeydb main.go

cluster-up:
	mkdir -p tmp/node{0,1,2}
	nohup ./monkeydb --mode=create --path=tmp/node0 > /dev/null 2>&1 &
	nohup ./monkeydb --mode=join --join=5000 --path=tmp/node1 > /dev/null 2>&1 &
	nohup ./monkeydb --mode=join --join=5000 --path=tmp/node2 > /dev/null 2>&1 &

cluster-down:
	pgrep -f monkeydb | xargs kill

.PHONY: clean build
.DEFAULT_GOAL := fcuk
