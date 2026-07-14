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
