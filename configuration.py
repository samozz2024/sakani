# Data collection categories
overview = True
mega_projects = True
projects_under_construction = True
projects_readymade = True
market_unit_buy = True
market_lands_buy = True
market_unit_rent = True

# Test mode: limits collection to first item per category
test_run = False

# Threading configuration
use_threading = True
max_workers = 2

# Rate limiting settings
pause_duration_minutes = 2  # Global pause duration when hitting 403/429 errors
speed_factor = 0.05  # Delay between requests in seconds <min = 0.2>
max_retries = 5

# Additional unit data (fetched for each unit)
unit_insights = True
unit_project_trends = True
unit_transactions = True

# Additional project data (fetched for each project)
project_insight = True
price_trends = True
project_transactions = True
demographics = True
