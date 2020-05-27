.PHONY: test
test:
	docker-compose -f docker-compose.yml up --build --abort-on-container-exit
	docker-compose -f docker-compose.yml down --volumes