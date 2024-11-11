all:
	@echo "Usage:" >&2
	@echo "    make download-local path=<PATH>: copy the dist folder of plakar-ui from a local directory" >&2
	@echo "    make download-last-release: download the latest GitHub release of plakar-ui" >&2

download-local:
	@if [ -z "$(path)" ]; \
	then \
		echo "Specify the path of plakar-ui with \"make download-local path=<PATH>\"" >&2; \
		exit 1; \
	fi

	$(eval PLAKAR_UI := $(realpath $(shell echo ${path})))

	@if [ ! -d "${PLAKAR_UI}" ] || [ ! -f "${PLAKAR_UI}/dist/index.html" ] || [ ! -d "${PLAKAR_UI}/dist/assets" ]; \
	then \
		echo "The path=<PATH> argument should point to plakar-ui containing a \"dist\" folder generated with \"npm run build\"" >&2; \
		exit 1; \
	fi

	rm -rf -- ./frontend/*
	echo local@$(shell git -C ~/dev/plakar/plakar-ui/ log -1 --format="%H") > ./frontend/VERSION
	cp -r ${PLAKAR_UI}/dist/* ./frontend/

download-last-release:
	echo todo