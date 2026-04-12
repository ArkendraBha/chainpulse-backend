from app.auth.auth import generate_access_token, hash_token

def test_generate_access_token_returns_string():
    assert isinstance(generate_access_token(), str)

def test_generate_access_token_is_long_enough():
    t = generate_access_token()
    assert len(t) >= 32

def test_generate_access_token_unique():
    t1 = generate_access_token()
    t2 = generate_access_token()
    assert t1 != t2

def test_hash_token_deterministic():
    t = generate_access_token()
    assert hash_token(t) == hash_token(t)

def test_hash_token_not_equal_original():
    t = generate_access_token()
    assert hash_token(t) != t
