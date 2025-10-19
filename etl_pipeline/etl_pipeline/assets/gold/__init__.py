from dagster import load_assets_from_modules


from .dimension import dim_gold_job_skills
from .dimension import dim_gold_job_industries
from .dimension import dim_gold_company_industries
from .dimension import dim_gold_company_specialities
from .dimension import dim_gold_employee_counts
from .dimension import dim_gold_job_benefits
from .dimension import dim_gold_job_salaries

from .fact import fact_gold_companies
from .fact import fact_gold_postings

gold_assets = load_assets_from_modules ([
    dim_gold_job_skills,
    dim_gold_job_industries,
    dim_gold_job_benefits,
    dim_gold_job_salaries,

    dim_gold_company_industries,
    dim_gold_company_specialities,
    dim_gold_employee_counts,
    
    
    fact_gold_companies,
    fact_gold_postings
])