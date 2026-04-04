PYTHON ?= python

.PHONY: install doctor bootstrap demo-data download extract panel episodes reaction lp figures descriptive publish site mvp offline-demo verify test clean

install:
	$(PYTHON) -m pip install -e ".[dev]"

doctor:
	$(PYTHON) -m coordwatch doctor

bootstrap:
	$(PYTHON) scripts/00_bootstrap.py

demo-data:
	$(PYTHON) scripts/17_build_demo_seed.py

download:
	$(PYTHON) scripts/01_download_fred.py --core --sectoral
	$(PYTHON) scripts/02_download_treasury_refunding.py --download-files
	$(PYTHON) scripts/03_download_treasury_financing.py --download-files
	$(PYTHON) scripts/04_download_buybacks.py --download-files
	$(PYTHON) scripts/05_download_primary_dealer.py --series PDPOSGST-TOT PDSORA-UTSETTOT PDSIRRA-UTSETTOT
	$(PYTHON) scripts/06_download_rates_and_repo.py
	$(PYTHON) scripts/22_download_daily_cash_debt.py

extract:
	$(PYTHON) scripts/07_build_refunding_statement_index.py
	$(PYTHON) scripts/08_extract_refunding_text.py

panel:
	$(PYTHON) scripts/09_build_refunding_panel.py
	$(PYTHON) scripts/10_build_weekly_master_panel.py

episodes:
	$(PYTHON) scripts/11_build_episode_registry.py

reaction:
	$(PYTHON) scripts/12_run_reaction_function.py

lp:
	$(PYTHON) scripts/13_run_local_projections.py

figures:
	$(PYTHON) scripts/14_build_figures.py

descriptive:
	$(PYTHON) scripts/20_build_descriptive_tables.py

publish:
	$(PYTHON) scripts/15_build_publish_artifacts.py

site:
	$(PYTHON) scripts/16_build_site_bundle_manifest.py

verify:
	$(PYTHON) scripts/verify_repo.py

mvp: bootstrap download extract panel episodes reaction lp descriptive publish site verify

offline-demo: bootstrap demo-data panel episodes reaction lp descriptive publish site verify

test:
	$(PYTHON) -B -m pytest tests/ -q

clean:
	rm -rf data/interim/* data/processed/* data/publish/* outputs/figures/* outputs/tables/* outputs/logs/* site/data/* site/figures/*
	touch data/interim/.gitkeep data/processed/.gitkeep data/publish/.gitkeep outputs/figures/.gitkeep outputs/tables/.gitkeep outputs/logs/.gitkeep site/data/.gitkeep site/figures/.gitkeep
