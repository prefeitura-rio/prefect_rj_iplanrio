# -*- coding: utf-8 -*-
"""
ACME DNS-01 + Cloud DNS (google) automation for Postfix certs.

Flow:
 - Download current cert from FTP
 - If it's not expiring within THRESHOLD_DAYS -> exit
 - Call certbot with dns-google plugin to issue cert for DOMAINS
 - Upload new cert (fullchain) and privkey to FTP
"""
import base64
import datetime
import hashlib
import os
import subprocess
import tempfile
from ftplib import FTP, error_perm
from io import BytesIO
from typing import Optional, Tuple

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import flow, task


@flow(log_prints=True)
def renew_certificate(
    domains: list[str],
    email: str,
    ftp_cert_path: str = "cert/fullchain.pem",
    ftp_key_path: str = "cert/privkey.pem",
    threshold_days: int = 30,
    certbot_propagation_seconds: int = 120,
) -> None:
    """
    Main certificate renewal flow.

    Args:
        domains: List of domains for certificate
        email: Contact email for ACME
        ftp_cert_path: Remote path for certificate (default: /cert/fullchain.pem)
        ftp_key_path: Remote path for private key (default: /cert/privkey.pem)
        threshold_days: Renew if cert expires within this many days (default: 30)
        certbot_propagation_seconds: DNS propagation wait time in seconds (default: 120)
    """
    # Get credentials from environment variables
    ftp_host = getenv_or_action("POSTFIX_CERTIFICATE_FTP_HOST")
    ftp_user = getenv_or_action("POSTFIX_CERTIFICATE_FTP_USER")
    ftp_pass = getenv_or_action("POSTFIX_CERTIFICATE_FTP_PASS")
    google_credentials_b64 = getenv_or_action("POSTFIX_CERTIFICATE_GOOGLE_CREDENTIALS_B64")

    print("Starting certificate renewal process")
    print(f"Domains: {', '.join(domains)}")
    print(f"Renewal threshold: {threshold_days} days")
    print(f"FTP server: {ftp_host}")

    # Decode service account key
    google_credentials_path = decode_service_account_key(google_credentials_b64)

    try:
        # Validate FTP credentials first to avoid unnecessary certbot API calls
        print("Validating FTP credentials...")
        validate_ftp_connection(ftp_host, ftp_user, ftp_pass)
        print("FTP credentials validated successfully")

        # Check current certificate
        current_cert, days_left = fetch_current_certificate(
            ftp_host, ftp_user, ftp_pass, ftp_cert_path
        )

        if days_left > threshold_days:
            print(
                f"Certificate expires in {days_left} days (threshold: {threshold_days}). "
                "No renewal needed."
            )
            return

        print(
            f"Certificate renewal required: {days_left} days left <= {threshold_days} day threshold"
        )

        # Issue new certificate
        with tempfile.TemporaryDirectory() as tmpdir:
            print(f"Using temporary directory: {tmpdir}")

            fullchain_path, privkey_path = issue_certificate_with_certbot(
                domains, email, google_credentials_path, certbot_propagation_seconds, tmpdir
            )

            new_fullchain, new_privkey = read_certificate_files(fullchain_path, privkey_path)

            # Check if certificate changed
            if current_cert and compare_certificates(current_cert, new_fullchain):
                print("New certificate identical to existing. Skipping upload.")
                return

            # Upload new certificates
            upload_certificates_to_ftp(
                ftp_host, ftp_user, ftp_pass,
                ftp_cert_path, ftp_key_path,
                new_fullchain, new_privkey
            )

        print("Certificate renewal completed successfully")
    finally:
        # Clean up temporary credentials file
        if os.path.exists(google_credentials_path):
            os.unlink(google_credentials_path)
            print(f"Cleaned up temporary credentials file: {google_credentials_path}")


@task
def decode_service_account_key(encoded_key: str) -> str:
    """
    Decode base64-encoded service account key and save to temporary file.

    Args:
        encoded_key: Base64-encoded JSON service account key

    Returns:
        Path to temporary file containing decoded credentials
    """
    print("Decoding service account key")
    decoded_key = base64.b64decode(encoded_key)

    temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.json')
    temp_file.write(decoded_key)
    temp_file.close()

    print(f"Service account key saved to temporary file: {temp_file.name}")
    return temp_file.name


@task
def calculate_sha256(data: bytes) -> str:
    """Calculate SHA256 hash of bytes."""
    return hashlib.sha256(data).hexdigest()


@task
def validate_ftp_connection(host: str, user: str, password: str, timeout: int = 60) -> None:
    """
    Validate FTP credentials by attempting to connect and login.

    Args:
        host: FTP server hostname
        user: FTP username
        password: FTP password
        timeout: Connection timeout in seconds

    Raises:
        RuntimeError: If FTP authentication fails
    """
    try:
        ftp = FTP(host, timeout=timeout)
        ftp.login(user, password)
        ftp.quit()
    except error_perm as e:
        error_msg = str(e)
        if "530" in error_msg or "Login" in error_msg:
            raise RuntimeError(
                f"FTP authentication failed: {error_msg}. "
                "Please check POSTFIX_CERTIFICATE_FTP_USER and POSTFIX_CERTIFICATE_FTP_PASS environment variables."
            ) from e
        raise RuntimeError(f"FTP permission error: {error_msg}") from e
    except Exception as e:
        raise RuntimeError(f"Failed to connect to FTP server {host}: {e}") from e


@task
def get_ftp_connection(host: str, user: str, password: str, timeout: int = 60) -> FTP:
    """Create and return an authenticated FTP connection."""
    print(f"Connecting to FTP server: {host}")
    ftp = FTP(host, timeout=timeout)
    ftp.login(user, password)
    print(f"Successfully authenticated to FTP server")
    return ftp


@task
def download_file_from_ftp(
    host: str, user: str, password: str, remote_path: str
) -> bytes:
    """
    Download a file from FTP server.

    Args:
        host: FTP server hostname
        user: FTP username
        password: FTP password
        remote_path: Remote file path to download

    Returns:
        File contents as bytes

    Raises:
        Exception: If download fails
    """
    print(f"Downloading file from FTP: {remote_path}")
    ftp = get_ftp_connection(host, user, password)
    buf = []
    try:
        ftp.retrbinary(f"RETR {remote_path}", buf.append)
    finally:
        ftp.quit()

    data = b"".join(buf)
    print(f"Downloaded {len(data)} bytes from {remote_path}")
    return data


@task
def ensure_ftp_directory(ftp: FTP, directory: str) -> None:
    """
    Ensure a directory exists on FTP server, create if needed.

    Args:
        ftp: Authenticated FTP connection
        directory: Directory path to ensure exists
    """
    if not directory:
        return

    print(f"Ensuring FTP directory exists: {directory}")
    try:
        ftp.mkd(directory)
        print(f"Created FTP directory: {directory}")
    except error_perm:
        print(f"FTP directory already exists: {directory}")


@task
def upload_file_to_ftp(
    host: str, user: str, password: str, remote_path: str, data: bytes
) -> None:
    """
    Upload a file to FTP server atomically using temp file + rename.

    Args:
        host: FTP server hostname
        user: FTP username
        password: FTP password
        remote_path: Remote file path to upload to
        data: File contents to upload

    Raises:
        Exception: If upload fails
    """
    print(f"Uploading {len(data)} bytes to FTP: {remote_path}")
    ftp = get_ftp_connection(host, user, password)

    try:
        dirname, basename = os.path.split(remote_path)
        ensure_ftp_directory(ftp, dirname)

        # Create temporary filename
        tmpname = basename + ".part-" + hashlib.sha1(data).hexdigest()[:8]
        tmp_remote = os.path.join(dirname, tmpname) if dirname else tmpname

        print(f"Uploading to temporary file: {tmp_remote}")
        bio = BytesIO(data)
        ftp.storbinary(f"STOR {tmp_remote}", bio)

        # Atomic rename
        print(f"Renaming {tmp_remote} to {remote_path}")
        try:
            ftp.rename(tmp_remote, remote_path)
            print(f"Successfully uploaded to {remote_path}")
        except error_perm:
            print("Rename failed, attempting delete then rename")
            try:
                ftp.delete(remote_path)
            except Exception as e:
                print(f"Could not delete existing file: {e}")
            ftp.rename(tmp_remote, remote_path)
            print(f"Successfully uploaded to {remote_path} (fallback method)")
    finally:
        ftp.quit()


@task
def parse_certificate_expiry(cert_pem: bytes) -> datetime.datetime:
    """
    Parse certificate and return expiration datetime.

    Args:
        cert_pem: Certificate in PEM format

    Returns:
        Certificate expiration datetime (timezone-aware UTC)
    """
    cert = x509.load_pem_x509_certificate(cert_pem, default_backend())
    return cert.not_valid_after_utc


@task
def calculate_days_until_expiry(cert_pem: bytes) -> int:
    """
    Calculate number of days until certificate expires.

    Args:
        cert_pem: Certificate in PEM format

    Returns:
        Number of days until expiry (0 if already expired)
    """
    expiry = parse_certificate_expiry(cert_pem)
    delta = expiry - datetime.datetime.now(datetime.timezone.utc)
    days = max(int(delta.total_seconds() // 86400), 0)
    print(f"Certificate expires in {days} days (expiry: {expiry.isoformat()})")
    return days


@task
def fetch_current_certificate(
    ftp_host: str, ftp_user: str, ftp_pass: str, cert_path: str
) -> Tuple[Optional[bytes], int]:
    """
    Fetch current certificate from FTP and calculate days until expiry.

    Args:
        ftp_host: FTP server hostname
        ftp_user: FTP username
        ftp_pass: FTP password
        cert_path: Remote path to certificate

    Returns:
        Tuple of (certificate_bytes, days_until_expiry)
        Returns (None, 0) if certificate cannot be fetched
    """
    print("Fetching current certificate from FTP")
    try:
        cert_data = download_file_from_ftp(ftp_host, ftp_user, ftp_pass, cert_path)
        days_left = calculate_days_until_expiry(cert_data)
        return cert_data, days_left
    except error_perm as e:
        error_msg = str(e)
        if "550" in error_msg and "No such file" in error_msg:
            print(f"Certificate file does not exist at {cert_path} (first run)")
            print("Will proceed to issue new certificate")
            return None, 0
        print(f"FTP permission error while fetching certificate: {e}")
        print("Will proceed to issue new certificate")
        return None, 0
    except Exception as e:
        print(f"Could not fetch existing certificate: {e}")
        print("Assuming no certificate exists or renewal needed")
        return None, 0


@task
def issue_certificate_with_certbot(
    domains: list[str],
    email: str,
    google_credentials_path: str,
    propagation_seconds: int,
    workdir: str,
) -> Tuple[str, str]:
    """
    Issue certificate using certbot with dns-google plugin.

    Args:
        domains: List of domains for certificate
        email: Contact email for ACME
        google_credentials_path: Path to Google service account JSON
        propagation_seconds: DNS propagation wait time
        workdir: Working directory for certbot

    Returns:
        Tuple of (fullchain_path, privkey_path)

    Raises:
        ValueError: If domain validation fails
        RuntimeError: If certbot fails or certificates not found
    """
    # Validate domains to prevent command injection
    import re
    dns_pattern = re.compile(r'^(\*\.)?[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*$')

    for domain in domains:
        if not domain or not isinstance(domain, str):
            raise ValueError(f"Invalid domain: domain must be a non-empty string, got {type(domain).__name__}")

        if not dns_pattern.match(domain):
            raise ValueError(
                f"Invalid domain format: '{domain}'. "
                "Domains must contain only letters, digits, hyphens, dots, and optionally start with '*.' for wildcards"
            )

        if len(domain) > 253:
            raise ValueError(f"Invalid domain: '{domain}' exceeds maximum length of 253 characters")

    print(f"Issuing certificate for domains: {', '.join(domains)}")

    # Setup directories
    config_dir = os.path.join(workdir, "config")
    logs_dir = os.path.join(workdir, "logs")
    os.makedirs(config_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    print(f"Certbot config dir: {config_dir}")
    print(f"Certbot logs dir: {logs_dir}")

    # Build certbot command
    domain_args = []
    for d in domains:
        domain_args += ["-d", d]

    cert_name = domains[0].replace("*", "wildcard").replace(".", "-")

    cmd = [
        "certbot", "certonly",
        "--non-interactive",
        "--agree-tos",
        "--email", email,
        "--dns-google",
        "--dns-google-credentials", google_credentials_path,
        "--dns-google-propagation-seconds", str(propagation_seconds),
        "--config-dir", config_dir,
        "--work-dir", workdir,
        "--logs-dir", logs_dir,
        "--preferred-challenges", "dns",
        "--keep-until-expiring",
    ] + domain_args

    # Set environment for certbot
    env = os.environ.copy()
    env["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials_path

    print(f"Running certbot with {propagation_seconds}s DNS propagation delay")
    print(f"Certbot command: {' '.join(cmd)}")

    # Execute certbot
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)

    # Log output
    if proc.stdout:
        print(f"Certbot stdout: {proc.stdout}")
    if proc.stderr:
        print(f"Certbot stderr: {proc.stderr}")

    if proc.returncode != 0:
        print(f"Certbot failed with return code {proc.returncode}")
        raise RuntimeError(f"certbot failed with code {proc.returncode}")

    print("Certbot completed successfully")

    # Find certificate files
    live_dir = os.path.join(config_dir, "live", cert_name)
    fullchain = os.path.join(live_dir, "fullchain.pem")
    privkey = os.path.join(live_dir, "privkey.pem")

    print(f"Looking for certificates at: {live_dir}")

    if not (os.path.exists(fullchain) and os.path.exists(privkey)):
        print("Certificates not at expected location, searching...")
        live_root = os.path.join(config_dir, "live")
        if os.path.isdir(live_root):
            entries = os.listdir(live_root)
            if entries:
                candidate = os.path.join(live_root, entries[0])
                fullchain = os.path.join(candidate, "fullchain.pem")
                privkey = os.path.join(candidate, "privkey.pem")
                print(f"Found certificates at: {candidate}")

    if not (os.path.exists(fullchain) and os.path.exists(privkey)):
        print("Certbot did not produce certificate files")
        raise RuntimeError("certbot did not produce certificate files")

    print(f"Certificate files located: fullchain={fullchain}, privkey={privkey}")
    return fullchain, privkey


@task
def read_certificate_files(fullchain_path: str, privkey_path: str) -> Tuple[bytes, bytes]:
    """
    Read certificate and private key files.

    Args:
        fullchain_path: Path to fullchain.pem
        privkey_path: Path to privkey.pem

    Returns:
        Tuple of (fullchain_bytes, privkey_bytes)
    """
    print("Reading certificate files")
    with open(fullchain_path, "rb") as f:
        fullchain = f.read()
    with open(privkey_path, "rb") as f:
        privkey = f.read()

    print(f"Read {len(fullchain)} bytes (fullchain) and {len(privkey)} bytes (privkey)")
    return fullchain, privkey


@task
def compare_certificates(old_cert: bytes, new_cert: bytes) -> bool:
    """
    Compare two certificates by SHA256 hash.

    Args:
        old_cert: Old certificate bytes
        new_cert: New certificate bytes

    Returns:
        True if certificates are identical, False otherwise
    """
    old_hash = calculate_sha256(old_cert)
    new_hash = calculate_sha256(new_cert)

    print(f"Old certificate SHA256: {old_hash}")
    print(f"New certificate SHA256: {new_hash}")

    identical = old_hash == new_hash
    if identical:
        print("Certificates are identical")
    else:
        print("Certificates differ")

    return identical


@task
def upload_certificates_to_ftp(
    ftp_host: str,
    ftp_user: str,
    ftp_pass: str,
    cert_path: str,
    key_path: str,
    cert_data: bytes,
    key_data: bytes,
) -> None:
    """
    Upload certificate and private key to FTP server.

    Args:
        ftp_host: FTP server hostname
        ftp_user: FTP username
        ftp_pass: FTP password
        cert_path: Remote path for certificate
        key_path: Remote path for private key
        cert_data: Certificate file contents
        key_data: Private key file contents
    """
    print("Uploading certificates to FTP")
    upload_file_to_ftp(ftp_host, ftp_user, ftp_pass, cert_path, cert_data)
    upload_file_to_ftp(ftp_host, ftp_user, ftp_pass, key_path, key_data)
    print("Certificates uploaded successfully")


