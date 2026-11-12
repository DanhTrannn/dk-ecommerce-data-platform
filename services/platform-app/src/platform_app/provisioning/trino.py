"""Generate Trino's private password file from injected client secrets."""

import hashlib
import os
from pathlib import Path

from platform_app.provisioning.state import atomic_private


def certificates():
    from datetime import UTC, datetime, timedelta
    from ipaddress import ip_address
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    root = Path("/run/trino-auth")
    public = Path("/run/trino-ca")
    public.mkdir(parents=True, exist_ok=True)
    public.chmod(0o755)
    ca_path = public / "ca.pem"
    if not (root / "server.pem").exists():
        now = datetime.now(UTC)
        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Retail Lakehouse Trino CA")])
        ca = (
            x509.CertificateBuilder()
            .subject_name(ca_name)
            .issuer_name(ca_name)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5))
            .not_valid_after(now + timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .sign(ca_key, hashes.SHA256())
        )
        key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "trino")]))
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5))
            .not_valid_after(now + timedelta(days=825))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.DNSName("trino"), x509.DNSName("localhost"), x509.IPAddress(ip_address("127.0.0.1"))]
                ),
                critical=False,
            )
            .sign(ca_key, hashes.SHA256())
        )
        pem = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
        atomic_private(
            root / "server.pem",
            (
                pem + cert.public_bytes(serialization.Encoding.PEM) + ca.public_bytes(serialization.Encoding.PEM)
            ).decode(),
            uid=1000,
            gid=1000,
        )
        ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        ca_path.chmod(0o644)
    if not ca_path.exists():
        raise ValueError("Trino CA is missing; restore the matching server/CA volumes")


def main():
    lines = []
    for user, variable in [("analytics_reader", "TRINO_READER_PASSWORD"), ("dbt_writer", "DBT_TRINO_PASSWORD")]:
        password = os.environ[variable]
        if not password:
            raise ValueError("Trino client secrets must be injected")
        salt = os.urandom(24)
        # Trino 483's PBKDF2 file authenticator uses HmacSHA1 (see its EncryptionUtil).
        digest = hashlib.pbkdf2_hmac("sha1", password.encode(), salt, 200000, dklen=32)
        lines.append(f"{user}:200000:{salt.hex()}:{digest.hex()}")
    atomic_private(Path("/run/trino-auth/password.db"), "\n".join(lines) + "\n", uid=1000, gid=1000)
    certificates()
    os.chown("/run/trino-auth", 1000, 1000)
    print("Trino authentication initialized")


if __name__ == "__main__":
    main()
