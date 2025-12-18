# Colors
COLOR_RESET   = \033[0m
COLOR_ERROR   = \033[1;31m
COLOR_SUCCESS = \033[1;32m
COLOR_INFO    = \033[1;36m
COLOR_TITLE   = \033[1;37m

.PHONY: help start restart stop

help:
	@echo "$(COLOR_TITLE)Available commands:$(COLOR_RESET)"
	@echo "$(COLOR_INFO) help$(COLOR_RESET)         Print more information)"
	@echo "$(COLOR_INFO) start$(COLOR_RESET)        Run docker container"
	@echo "$(COLOR_INFO) stop$(COLOR_RESET)         Stop docker container"
	@echo "$(COLOR_INFO) restart$(COLOR_RESET)      Restart docker container"

start:
	@echo "$(COLOR_INFO)Starting docker container...$(COLOR_RESET)"
	@sudo docker-compose up --build --force-recreate

restart:
	@echo "$(COLOR_INFO)Restarting docker container...$(COLOR_RESET)"
	@sudo docker-compose down && sudo docker-compose up --build

stop:
	@echo "$(COLOR_INFO)Stoping docker container...$(COLOR_RESET)"
	@sudo docker-compose down
