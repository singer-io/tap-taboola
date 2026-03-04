"""
Realistic fixture data matching the Taboola API response format and
tap_taboola/schemas.py field definitions.
"""

# ---------------------------------------------------------------------------
# Campaign fixtures — Taboola GET /backstage/api/1.0/{account}/campaigns/
# ---------------------------------------------------------------------------

CAMPAIGNS = [
    {
        'id': '1001',
        'advertiser_id': 'acme-advertiser',
        'name': 'Campaign Alpha',
        'tracking_code': 'trk-alpha',
        'cpc': '0.35',
        'daily_cap': '150.0',
        'spending_limit': '3000.0',
        'spending_limit_model': 'MONTHLY',
        'country_targeting': {'type': 'INCLUDE', 'value': ['US', 'CA']},
        'platform_targeting': {'type': 'INCLUDE', 'value': ['DESK']},
        'publisher_targeting': None,
        'start_date': '2023-01-01',
        'end_date': None,
        'approval_state': 'APPROVED',
        'is_active': True,
        'spent': '820.50',
        'status': 'RUNNING',
    },
    {
        'id': '1002',
        'advertiser_id': 'acme-advertiser',
        'name': 'Campaign Beta',
        'tracking_code': 'trk-beta',
        'cpc': '0.50',
        'daily_cap': '200.0',
        'spending_limit': '5000.0',
        'spending_limit_model': 'ENTIRE',
        'country_targeting': None,
        'platform_targeting': None,
        'publisher_targeting': None,
        'start_date': '2023-06-01',
        'end_date': '2024-12-31',
        'approval_state': 'APPROVED',
        'is_active': True,
        'spent': '1200.00',
        'status': 'RUNNING',
    },
    {
        'id': '1003',
        'advertiser_id': 'acme-advertiser',
        'name': 'Campaign Gamma',
        'tracking_code': '',
        'cpc': '0.20',
        'daily_cap': '50.0',
        'spending_limit': '500.0',
        'spending_limit_model': 'MONTHLY',
        'country_targeting': {'type': 'EXCLUDE', 'value': ['CN']},
        'platform_targeting': None,
        'publisher_targeting': {'type': 'INCLUDE', 'value': ['pub-001']},
        'start_date': '2024-03-15',
        'end_date': None,
        'approval_state': 'PENDING',
        'is_active': False,
        'spent': '0.0',
        'status': 'PAUSED',
    },
    {
        'id': '1004',
        'advertiser_id': 'acme-advertiser',
        'name': 'Campaign Delta',
        'tracking_code': 'trk-delta',
        'cpc': '0.75',
        'daily_cap': '500.0',
        'spending_limit': '10000.0',
        'spending_limit_model': 'MONTHLY',
        'country_targeting': None,
        'platform_targeting': {'type': 'INCLUDE', 'value': ['PHON', 'TBLT']},
        'publisher_targeting': None,
        'start_date': None,
        'end_date': None,
        'approval_state': 'APPROVED',
        'is_active': True,
        'spent': '3450.75',
        'status': 'RUNNING',
    },
    {
        'id': '1005',
        'advertiser_id': 'acme-advertiser',
        'name': 'Campaign Epsilon',
        'tracking_code': 'trk-eps',
        'cpc': '0.10',
        'daily_cap': '25.0',
        'spending_limit': '200.0',
        'spending_limit_model': 'MONTHLY',
        'country_targeting': None,
        'platform_targeting': None,
        'publisher_targeting': None,
        'start_date': '2025-01-01',
        'end_date': '2025-06-30',
        'approval_state': 'REJECTED',
        'is_active': False,
        'spent': '15.20',
        'status': 'STOPPED',
    },
]

CAMPAIGNS_RESPONSE = {'results': CAMPAIGNS}


# ---------------------------------------------------------------------------
# Campaign performance fixtures
# Taboola GET .../reports/campaign-summary/dimensions/campaign_day_breakdown
# Rows span 2023-01-01 through 2024-10-31 (early) and 2024-11-01+ (late).
# ---------------------------------------------------------------------------

def _perf_row(campaign_id, date_str, impressions, clicks, spent,
              ctr=None, cpc=None, currency='USD'):
    """Helper — build a raw API performance row."""
    imp = int(impressions)
    clk = int(clicks)
    sp = float(spent)
    _ctr = round(clk / imp, 6) if imp else 0.0
    _cpc = round(sp / clk, 4) if clk else 0.0
    return {
        'campaign': str(campaign_id),
        'campaign_name': f'Campaign {campaign_id}',
        'date': f'{date_str} 00:00:00.000000',
        'impressions': str(imp),
        'clicks': str(clk),
        'ctr': str(ctr if ctr is not None else _ctr),
        'cpc': str(cpc if cpc is not None else _cpc),
        'cpa_actions_num': '5',
        'cpa': '2.50',
        'cpm': str(round(sp / imp * 1000, 4)) if imp else '0.0',
        'cpa_conversion_rate': '0.01',
        'spent': str(sp),
        'currency': currency,
        'conversions_value': str(round(sp * 1.2, 2)),
    }


# 40 rows spanning 2023 and early 2024 (for start_date_1 = 2023-01-01)
_EARLY_ROWS = [
    _perf_row(1001, '2023-01-15', 50000, 250, 87.50),
    _perf_row(1001, '2023-02-10', 48000, 240, 84.00),
    _perf_row(1001, '2023-03-05', 55000, 275, 96.25),
    _perf_row(1001, '2023-04-20', 60000, 300, 105.00),
    _perf_row(1001, '2023-05-18', 45000, 225, 78.75),
    _perf_row(1001, '2023-06-22', 52000, 260, 91.00),
    _perf_row(1001, '2023-07-14', 47000, 235, 82.25),
    _perf_row(1001, '2023-08-09', 53000, 265, 92.75),
    _perf_row(1001, '2023-09-03', 49000, 245, 85.75),
    _perf_row(1001, '2023-10-27', 58000, 290, 101.50),
    _perf_row(1002, '2023-06-15', 80000, 400, 200.00),
    _perf_row(1002, '2023-07-20', 75000, 375, 187.50),
    _perf_row(1002, '2023-08-11', 90000, 450, 225.00),
    _perf_row(1002, '2023-09-08', 70000, 350, 175.00),
    _perf_row(1002, '2023-10-14', 85000, 425, 212.50),
    _perf_row(1002, '2023-11-19', 95000, 475, 237.50),
    _perf_row(1002, '2023-12-25', 100000, 500, 250.00),
    _perf_row(1003, '2023-03-01', 20000, 100, 20.00),
    _perf_row(1003, '2023-04-15', 22000, 110, 22.00),
    _perf_row(1003, '2023-05-30', 18000, 90, 18.00),
    _perf_row(1004, '2023-07-04', 120000, 600, 450.00),
    _perf_row(1004, '2023-08-15', 130000, 650, 487.50),
    _perf_row(1004, '2023-09-22', 110000, 550, 412.50),
    _perf_row(1004, '2023-10-31', 125000, 625, 468.75),
    _perf_row(1004, '2023-11-11', 140000, 700, 525.00),
    _perf_row(1004, '2023-12-01', 135000, 675, 506.25),
    _perf_row(1001, '2024-01-10', 54000, 270, 94.50),
    _perf_row(1001, '2024-02-14', 51000, 255, 89.25),
    _perf_row(1001, '2024-03-20', 56000, 280, 98.00),
    _perf_row(1002, '2024-01-05', 88000, 440, 220.00),
    _perf_row(1002, '2024-02-28', 92000, 460, 230.00),
    _perf_row(1002, '2024-03-15', 78000, 390, 195.00),
    _perf_row(1003, '2024-03-16', 25000, 125, 25.00),
    _perf_row(1003, '2024-04-10', 27000, 135, 27.00),
    _perf_row(1004, '2024-01-20', 145000, 725, 543.75),
    _perf_row(1004, '2024-02-08', 138000, 690, 517.50),
    _perf_row(1004, '2024-03-25', 150000, 750, 562.50),
    _perf_row(1004, '2024-04-30', 142000, 710, 532.50),
    _perf_row(1005, '2024-01-15', 15000, 75, 7.50),
    _perf_row(1005, '2024-02-20', 12000, 60, 6.00),
]

# 10 rows from 2024-11-01 onward (for start_date_2 = 2024-10-01)
_LATE_ROWS = [
    _perf_row(1001, '2024-11-05', 57000, 285, 99.75),
    _perf_row(1001, '2024-12-20', 62000, 310, 108.50),
    _perf_row(1002, '2024-11-15', 96000, 480, 240.00),
    _perf_row(1002, '2024-12-10', 102000, 510, 255.00),
    _perf_row(1004, '2024-11-08', 155000, 775, 581.25),
    _perf_row(1004, '2024-12-01', 160000, 800, 600.00),
    _perf_row(1001, '2025-01-15', 59000, 295, 103.25),
    _perf_row(1002, '2025-01-20', 98000, 490, 245.00),
    _perf_row(1004, '2025-01-25', 162000, 810, 607.50),
    _perf_row(1004, '2025-02-10', 158000, 790, 592.50),
]

# Full dataset returned when start_date is 2023-01-01 (all rows)
ALL_PERFORMANCE_ROWS = _EARLY_ROWS + _LATE_ROWS

# Subset returned when start_date is 2024-10-01 (only late rows)
LATE_PERFORMANCE_ROWS = _LATE_ROWS

ALL_PERFORMANCE_RESPONSE = {'results': ALL_PERFORMANCE_ROWS}
LATE_PERFORMANCE_RESPONSE = {'results': LATE_PERFORMANCE_ROWS}


# ---------------------------------------------------------------------------
# Token / account fixtures
# ---------------------------------------------------------------------------

TOKEN_RESPONSE = {'access_token': 'mock-access-token-abc123'}
TOKEN_DETAILS_RESPONSE = {'account_id': 'test-account-id'}
