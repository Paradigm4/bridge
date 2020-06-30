all:
	$(MAKE) -C src
	@cp src/*.so .

test:


clean:
	$(MAKE) -C src clean
	rm -f *.so
