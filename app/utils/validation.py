import ipaddress
import socket
from urllib.parse import urlparse
from fastapi import HTTPException


def validate_webhook_url(url: str) -> str:
    """
    FIX 3: SSRF-safe webhook URL validation.
    Blocks private IPs, localhost, metadata endpoints,
    credentials in URLs, and non-HTTPS schemes.
    """
    if not url or not url.startswith("https://"):
        raise HTTPException(
            status_code=400,
            detail="Webhook URL must use HTTPS",
        )

    try:
        parsed = urlparse(url)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid URL",
        )

    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(
            status_code=400,
            detail="Invalid hostname",
        )

    # Block known dangerous hostnames
    blocked = {
        "metadata.google.internal",
        "localhost",
        "169.254.169.254",
    }
    if hostname.lower() in blocked:
        raise HTTPException(
            status_code=400,
            detail="Blocked hostname",
        )

    # Block private IP ranges
    blocked_nets = [
        ipaddress.ip_network(n)
        for n in [
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "127.0.0.0/8",
            "169.254.0.0/16",
        ]
    ]

    try:
        for info in socket.getaddrinfo(hostname, None):
            ip = ipaddress.ip_address(info[4][0])
            for net in blocked_nets:
                if ip in net:
                    raise HTTPException(
                        status_code=400,
                        detail="Webhook resolves to private IP",
                    )
    except socket.gaierror:
        raise HTTPException(
            status_code=400,
            detail="Cannot resolve hostname",
        )

    # Block credentials in URL
    if parsed.username or parsed.password:
        raise HTTPException(
            status_code=400,
            detail="URL cannot contain credentials",
        )

    return url


import re
import html as _html


def sanitize_string(value: str, max_length: int = 500) -> str:
    """Sanitizes free-text string inputs."""
    if not value:
        return value
    value = _html.escape(value)
    value = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", value)
    return value[:max_length].strip()


def sanitize_coin(coin: str) -> str:
    """Validates and normalizes coin symbol."""
    from app.core.config import settings
    from fastapi import HTTPException

    coin = coin.upper().strip()
    if coin not in settings.SUPPORTED_COINS:
        raise HTTPException(400, detail=f"Unsupported coin: {coin}")
    return coin
