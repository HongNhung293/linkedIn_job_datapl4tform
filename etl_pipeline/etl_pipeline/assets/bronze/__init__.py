from dagster import load_assets_from_modules

from . import bronze_linkedIn_companies
from . import bronze_linkedIn_jobs
from . import bronze_linkedIn_mapping
from . import bronze_linkedIn_postings

bronze_assets = load_assets_from_modules([
    bronze_linkedIn_companies,
    bronze_linkedIn_jobs,
    bronze_linkedIn_mapping,
    bronze_linkedIn_postings,
])
