from dagster import load_assets_from_modules


from . import silver_linkedIn_companies
from . import silver_linkedIn_jobs
from . import silver_linkedIn_mapping
from . import silver_linkedIn_postings


silver_assets = load_assets_from_modules ([
    silver_linkedIn_companies,
    silver_linkedIn_jobs,
    silver_linkedIn_mapping,
    silver_linkedIn_postings
])