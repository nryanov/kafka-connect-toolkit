clean:
	./gradlew clean

compile:
	./gradlew compileJava

package:
	./gradlew jar

docs-serve:
	python3 -m venv .venv-docs && .venv-docs/bin/pip install -r requirements-docs.txt && .venv-docs/bin/mkdocs serve

docs-build:
	python3 -m venv .venv-docs && .venv-docs/bin/pip install -r requirements-docs.txt && .venv-docs/bin/mkdocs build --strict
