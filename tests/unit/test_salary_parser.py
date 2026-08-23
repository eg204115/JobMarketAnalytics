from transformation.salary_parser import _parse_single_salary_text


def test_parses_dollar_range():
    low, high, currency = _parse_single_salary_text("$90,000 - $120,000 a year")
    assert low == 90000.0
    assert high == 120000.0
    assert currency == "USD"


def test_parses_single_value_no_range():
    low, high, currency = _parse_single_salary_text("$85,000 annually")
    assert low == 85000.0
    assert high == 85000.0
    assert currency == "USD"


def test_parses_k_shorthand():
    low, high, currency = _parse_single_salary_text("£45k - £60k")
    assert low == 45000.0
    assert high == 60000.0
    assert currency == "GBP"


def test_returns_none_for_missing_text():
    assert _parse_single_salary_text(None) == (None, None, None)


def test_returns_none_when_no_salary_pattern_found():
    low, high, currency = _parse_single_salary_text("Competitive salary, negotiable")
    assert (low, high, currency) == (None, None, None)


def test_comma_only_amount_does_not_crash():
    # "[\d,]+" matched a bare comma, so float("") raised ValueError and killed
    # the whole Spark stage rather than skipping one unparseable posting.
    assert _parse_single_salary_text("$, negotiable") == (None, None, None)


def test_stray_k_elsewhere_in_text_does_not_scale_the_amount():
    # "k" in text.lower() fired on any k anywhere — "Bank" here — turning an
    # hourly rate of 500 into 500000.
    low, high, currency = _parse_single_salary_text("Rs. 500 per hour at Bank of Ceylon")
    assert low == 500.0
    assert high == 500.0
    assert currency == "LKR"


def test_k_suffix_scales_only_the_number_it_follows():
    low, high, _ = _parse_single_salary_text("$50k - $75,000")
    assert low == 50000.0
    assert high == 75000.0
