.PHONY: lint
lint: ## runs code cleanup on modified C# files from current branch commits
	@echo "Running code cleanup on modified files..."
	@dotnet tool restore > /dev/null
	@FILES_TMP=$$(mktemp); \
	echo "Formatting all files changed in current branch..."; \
	TARGET_BRANCH=""; \
	if git rev-parse --verify origin/master >/dev/null 2>&1; then \
		TARGET_BRANCH="origin/master"; \
	elif git rev-parse --verify origin/main >/dev/null 2>&1; then \
		TARGET_BRANCH="origin/main"; \
	fi; \
	if [ -n "$$TARGET_BRANCH" ]; then \
		git log --name-only --pretty=format: "$${TARGET_BRANCH}..HEAD" | grep '\.cs$$' | sort -u >> $$FILES_TMP || true; \
	else \
		echo "Warning: Could not find origin/master or origin/main branch"; \
		exit 1; \
	fi; \
	git diff --name-only --cached | grep '\.cs$$' >> $$FILES_TMP || true; \
    git diff --name-only | grep '\.cs$$' >> $$FILES_TMP || true; \
	FILES=$$(sort -u $$FILES_TMP); \
	rm -f $$FILES_TMP; \
	if [ -z "$$FILES" ]; then \
		echo "No modified C# files to format."; \
		exit 0; \
	fi; \
	FILES_EXISTING=""; \
	for file in $$FILES; do \
		if [ -f "$$file" ]; then \
			FILES_EXISTING="$$FILES_EXISTING$$file"$$'\n'; \
		fi; \
	done; \
	if [ -z "$$FILES_EXISTING" ]; then \
		echo "No existing C# files to format (files may have been deleted)."; \
		exit 0; \
	fi; \
	FILE_COUNT=$$(echo "$$FILES_EXISTING" | grep -c .); \
	echo "Found $$FILE_COUNT modified C# file(s) to format"; \
	INCLUDE_LIST=$$(echo "$$FILES_EXISTING" | tr '\n' ';' | sed 's/;$$//'); \
	if [ -n "$$INCLUDE_LIST" ]; then \
		dotnet jb cleanupcode \
			--profile="Reformat Code" \
			--disable-settings-layers=GlobalAll,GlobalPerProduct,SolutionPersonal \
			--settings="Kuna.Projections.sln.DotSettings" \
			--exclude="**/migrations/**" \
			--include="$$INCLUDE_LIST" \
			--verbosity=ERROR \
			--no-build \
			Kuna.Projections.sln; \
	fi; \
	echo "Code cleanup complete!"
