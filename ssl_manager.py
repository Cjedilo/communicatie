"""
SSL certificate management.

Start-up order:
1. If real cert files exist (SSL_CERT / SSL_KEY) and are valid → use them.
2. Otherwise generate a self-signed cert and save to SSL_DIR.

Upgrade to Let's Encrypt:
  python ssl_manager.py --letsencrypt --domain example.com --email admin@example.com
This runs an ACME HTTP-01 challenge, writes real certs, and exits.
After that, restart the server — step 1 above picks them up.

Renewal (add to cron):
  python ssl_manager.py --renew
"""

import argparse
import ipaddress
import logging
import os
import ssl
import datetime
from pathlib import Path

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Self-signed generation
# ---------------------------------------------------------------------------

def _generate_self_signed(cert_path: Path, key_path: Path, hostname: str = "localhost"):
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except ImportError:
        raise RuntimeError("pip install cryptography  (needed for self-signed cert generation)")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, hostname),
    ])

    san_list = [x509.DNSName(hostname)]
    if hostname != "localhost":
        san_list.append(x509.DNSName("localhost"))
    try:
        san_list.append(x509.IPAddress(ipaddress.ip_address(hostname)))
    except ValueError:
        pass

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3650))
        .add_extension(x509.SubjectAlternativeName(san_list), critical=False)
        .sign(key, hashes.SHA256())
    )

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    log.info("Self-signed certificate written to %s", cert_path)


# ---------------------------------------------------------------------------
# Let's Encrypt via acme
# ---------------------------------------------------------------------------

def _letsencrypt(domain: str, email: str, cert_path: Path, key_path: Path, staging: bool):
    """
    Minimal ACME HTTP-01 flow using the `acme` + `cryptography` packages.
    Requires port 80 to be reachable.
    """
    try:
        import acme.challenges
        import acme.client
        import acme.crypto_util
        import acme.messages
        import josepy
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization, hashes
        from cryptography import x509
        from cryptography.x509.oid import NameOID
    except ImportError:
        raise RuntimeError("pip install acme cryptography  (needed for Let's Encrypt)")

    import json, threading, time
    from http.server import HTTPServer, BaseHTTPRequestHandler

    DIRECTORY = (
        "https://acme-staging-v02.api.letsencrypt.org/directory"
        if staging
        else "https://acme-v02.api.letsencrypt.org/directory"
    )

    acc_key = josepy.JWKRSA(
        key=rsa.generate_private_key(public_exponent=65537, key_size=2048)
    )

    net = acme.client.ClientNetwork(acc_key, user_agent="communicatie/1.0")
    directory = acme.messages.Directory.from_json(net.get(DIRECTORY).json())
    client = acme.client.ClientV2(directory, net)
    client.new_account(acme.messages.NewRegistration.from_data(email=email, terms_of_service_agreed=True))

    domain_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, domain)]))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(domain)]), critical=False)
        .sign(domain_key, hashes.SHA256())
    )
    csr_pem = csr.public_bytes(serialization.Encoding.PEM)

    order = client.new_order(csr_pem)
    challenge_body = None
    for auth in order.authorizations:
        for ch in auth.body.challenges:
            if isinstance(ch.chall, acme.challenges.HTTP01):
                challenge_body = ch
                break

    token = challenge_body.chall.token
    response, validation = challenge_body.response_and_validation(acc_key)

    # Serve the challenge token temporarily on port 80
    challenge_path = f"/.well-known/acme-challenge/{token.decode()}"
    challenge_body_bytes = validation.encode()

    class ChallengeHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == challenge_path:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(challenge_body_bytes)
            else:
                self.send_response(404)
                self.end_headers()
        def log_message(self, *_): pass

    httpd = HTTPServer(("", 80), ChallengeHandler)
    t = threading.Thread(target=httpd.serve_forever)
    t.daemon = True
    t.start()

    try:
        client.answer_challenge(challenge_body, response)
        finalized = client.poll_and_finalize(order)
    finally:
        httpd.shutdown()

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    cert_path.write_text(finalized.fullchain_pem)
    key_path.write_bytes(
        domain_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    log.info("Let's Encrypt certificate written to %s", cert_path)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_active_ctx: ssl.SSLContext | None = None


def reload_cert_chain(cert_path: str, key_path: str):
    """Hot-reload the cert into the running SSL context — no restart needed."""
    if _active_ctx:
        _active_ctx.load_cert_chain(cert_path, key_path)


def ensure_ssl(cert_path_str: str, key_path_str: str, hostname: str = "localhost") -> ssl.SSLContext:
    """
    Returns a server SSLContext.  Generates a self-signed cert if none exists.
    """
    cert_path = Path(cert_path_str)
    key_path  = Path(key_path_str)

    if not cert_path.exists() or not key_path.exists():
        log.warning("No SSL certificate found — generating self-signed cert for %s", hostname)
        _generate_self_signed(cert_path, key_path, hostname)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert_path, key_path)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    global _active_ctx
    _active_ctx = ctx
    return ctx


def peer_ssl_context(fingerprint: str | None = None) -> ssl.SSLContext:
    """
    SSLContext for outbound peer connections.
    Always CERT_NONE — we do our own TOFU fingerprint check in connect_peer().
    System CA verification would reject self-signed certs.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def cert_fingerprint(cert_der: bytes) -> str:
    import hashlib
    return hashlib.sha256(cert_der).hexdigest()


def sign_succession(old_key_path: str, old_cert_path: str, new_cert_path: str) -> dict | None:
    """
    Sign a succession record: the old private key vouches for the new cert.
    Returns the record dict ready for storage, or None if fingerprints are identical
    or if signing fails.
    """
    import base64, json as _json
    from cryptography import x509 as cx
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as _pad
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from datetime import datetime, timezone

    try:
        old_cert_der = cx.load_pem_x509_certificate(
            Path(old_cert_path).read_bytes()
        ).public_bytes(serialization.Encoding.DER)
        new_cert_der = cx.load_pem_x509_certificate(
            Path(new_cert_path).read_bytes()
        ).public_bytes(serialization.Encoding.DER)
    except Exception as e:
        log.warning("sign_succession: cannot load certs: %s", e)
        return None

    old_fp = cert_fingerprint(old_cert_der)
    new_fp = cert_fingerprint(new_cert_der)
    if old_fp == new_fp:
        return None

    timestamp = datetime.now(timezone.utc).isoformat()
    payload = _json.dumps(
        {"old_fp": old_fp, "new_fp": new_fp, "timestamp": timestamp}, sort_keys=True
    ).encode()

    try:
        private_key = load_pem_private_key(Path(old_key_path).read_bytes(), password=None)
        signature   = private_key.sign(payload, _pad.PKCS1v15(), hashes.SHA256())
    except Exception as e:
        log.warning("sign_succession: cannot sign: %s", e)
        return None

    return {
        "old_fingerprint":  old_fp,
        "old_cert_der_b64": base64.b64encode(old_cert_der).decode(),
        "new_fingerprint":  new_fp,
        "new_cert_der_b64": base64.b64encode(new_cert_der).decode(),
        "timestamp":        timestamp,
        "signature":        base64.b64encode(signature).decode(),
    }


def sign_succession_bytes(old_key_pem: bytes, old_cert_der: bytes, new_cert_der: bytes) -> dict | None:
    """
    Like sign_succession but takes raw bytes — use when the old key/cert are
    already in memory (e.g. read before overwriting during renewal).
    """
    import base64, json as _json
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as _pad
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from datetime import datetime, timezone

    old_fp = cert_fingerprint(old_cert_der)
    new_fp = cert_fingerprint(new_cert_der)
    if old_fp == new_fp:
        return None

    timestamp = datetime.now(timezone.utc).isoformat()
    payload = _json.dumps(
        {"old_fp": old_fp, "new_fp": new_fp, "timestamp": timestamp}, sort_keys=True
    ).encode()

    try:
        private_key = load_pem_private_key(old_key_pem, password=None)
        signature   = private_key.sign(payload, _pad.PKCS1v15(), hashes.SHA256())
    except Exception as e:
        log.warning("sign_succession_bytes: cannot sign: %s", e)
        return None

    from cryptography import x509 as cx
    new_cert_der_canon = cx.load_der_x509_certificate(new_cert_der).public_bytes(serialization.Encoding.DER)

    return {
        "old_fingerprint":  old_fp,
        "old_cert_der_b64": base64.b64encode(old_cert_der).decode(),
        "new_fingerprint":  new_fp,
        "new_cert_der_b64": base64.b64encode(new_cert_der_canon).decode(),
        "timestamp":        timestamp,
        "signature":        base64.b64encode(signature).decode(),
    }


def verify_succession_step(record: dict) -> bool:
    """
    Verify one link in a succession chain. Returns True only if:
    - old_cert_der hashes to old_fingerprint
    - new_cert_der hashes to new_fingerprint
    - signature over {old_fp, new_fp, timestamp} is valid under old_cert's public key
    """
    import base64, json as _json
    from cryptography import x509 as cx
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding as _pad

    try:
        old_cert_der = base64.b64decode(record["old_cert_der_b64"])
        new_cert_der = base64.b64decode(record["new_cert_der_b64"])

        if cert_fingerprint(old_cert_der) != record["old_fingerprint"]:
            return False
        if cert_fingerprint(new_cert_der) != record["new_fingerprint"]:
            return False

        public_key = cx.load_der_x509_certificate(old_cert_der).public_key()
        payload = _json.dumps(
            {"old_fp": record["old_fingerprint"],
             "new_fp": record["new_fingerprint"],
             "timestamp": record["timestamp"]},
            sort_keys=True,
        ).encode()
        public_key.verify(
            base64.b64decode(record["signature"]), payload, _pad.PKCS1v15(), hashes.SHA256()
        )
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# CLI — run directly to manage certs
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import config
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Communicatie SSL manager")
    sub = parser.add_subparsers(dest="cmd")

    le = sub.add_parser("letsencrypt", help="Obtain a Let's Encrypt certificate")
    le.add_argument("--domain",  required=True)
    le.add_argument("--email",   required=True)
    le.add_argument("--staging", action="store_true")

    sub.add_parser("self-signed", help="Generate a self-signed certificate")
    sub.add_parser("info",        help="Show current certificate info")

    args = parser.parse_args()

    cert = Path(config.SSL_CERT)
    key  = Path(config.SSL_KEY)

    if args.cmd == "letsencrypt":
        _letsencrypt(args.domain, args.email, cert, key, args.staging)

    elif args.cmd == "self-signed":
        _generate_self_signed(cert, key)

    elif args.cmd == "info":
        if cert.exists():
            from cryptography import x509 as cx
            data = cx.load_pem_x509_certificate(cert.read_bytes())
            print(f"Subject : {data.subject}")
            print(f"Issuer  : {data.issuer}")
            print(f"Expires : {data.not_valid_after_utc}")
        else:
            print("No certificate found at", cert)
    else:
        parser.print_help()
