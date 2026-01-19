import warnings

# Aggressively silence the macOS LibreSSL warning from urllib3
# This must happen before other imports trigger the warning.
try:
    from urllib3.exceptions import NotOpenSSLWarning
    warnings.simplefilter("ignore", NotOpenSSLWarning)
except ImportError:
    pass  # urllib3 might not be installed or version differs, ignore.

# Also suppress the specific UserWarning text just in case
warnings.filterwarnings(
    "ignore",
    message="urllib3 v2 only supports OpenSSL 1.1.1+",
    category=UserWarning
)
