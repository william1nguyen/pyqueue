.PHONY: help install test bench clean
.DEFAULT_GOAL := help

help:
	@echo "PyQueue - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $1, $2}'
	@echo ""

install:
	uv sync

test:
	uv run pytest src/tests/test_queue.py -v

coverage:
	uv run pytest src/tests/ --cov=src --cov-report=html --cov-report=term-missing

benchmark:
	uv run python -m src.benchmark

clean:
	@echo "$(BLUE)Cleaning up...$(NC)"
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf **/__pycache__
	rm -rf src/**/*.pyc
	rm -rf *.egg-info
	rm -rf dist
	rm -rf build
	rm -rf load_test_results.json
	@echo "$(GREEN)Cleaned$(NC)"


stats:
	@echo "$(BLUE)Project Statistics$(NC)"
	@echo "$(BLUE)==================$(NC)"
	@echo ""
	@echo "$(GREEN)Lines of code:$(NC)"
	@find src -name "*.py" -not -path "*/tests/*" | xargs wc -l | tail -1
	@echo ""
	@echo "$(GREEN)Test files:$(NC)"
	@find src/tests -name "test_*.py" | wc -l | xargs echo "  Files:"
	@find src/tests -name "test_*.py" | xargs wc -l | tail -1 | awk '{print "  Lines: " $$1}'
	@echo ""
	@echo "$(GREEN)Test count:$(NC)"
	@uv run pytest src/tests/ --collect-only -q 2>/dev/null | grep "test session" || echo "  Run 'make test' first"

t: test ## Alias for test
tc: test-coverage ## Alias for coverage
b: benchmark ## Alias for benchmark
r: redis-start ## Alias for redis-start
c: clean ## Alias for clean