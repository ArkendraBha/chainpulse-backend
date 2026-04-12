from app.services.market_data import classify, regime_shift_risk, volatility, calculate_coherence

def test_classify_strong_risk_on():
    assert classify(50) == "Strong Risk-On"

def test_classify_risk_on():
    assert classify(20) == "Risk-On"

def test_classify_neutral():
    assert classify(0) == "Neutral"

def test_classify_risk_off():
    assert classify(-20) == "Risk-Off"

def test_classify_strong_risk_off():
    assert classify(-50) == "Strong Risk-Off"

def test_regime_shift_risk_bounds():
    val = regime_shift_risk(90, 20, 30)
    assert 0 <= val <= 100

def test_volatility_zero_for_flat_prices():
    prices = [100] * 30
    assert volatility(prices) == 0

def test_coherence_bounds():
    val = calculate_coherence(2, 4, 10)
    assert 0 <= val <= 100
