from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol, runtime_checkable, Any


@runtime_checkable
class ComplianceValidator(Protocol):
    """Protocol for storage compliance validators."""

    def validate(self, config: dict[str, Any]) -> "ValidationResult": ...


class EncryptionStandard(Enum):
    """Supported encryption standards."""

    AES_256_GCM = "AES-256-GCM"
    ChaCha20_Poly1305 = "ChaCha20-Poly1305"
    RSA_OAEP_4096 = "RSA-OAEP-4096"  # Key wrapping
    NONE = "NONE"


class TransportSecurity(Enum):
    """Supported transport security protocols."""

    TLS_1_2 = "TLSv1.2"
    TLS_1_3 = "TLSv1.3"
    PLAIN = "PLAIN"


@dataclass
class ValidationResult:
    """Result of a compliance validation check."""

    compliant: bool
    standards_met: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    remediation_steps: list[str] = field(default_factory=list)


class StorageCompliance:
    """
    Enforces NIST SP 800-175B and ISO/IEC 19772 compliance for storage operations.
    """

    REQUIRED_ENCRYPTION = {EncryptionStandard.AES_256_GCM, EncryptionStandard.ChaCha20_Poly1305}

    REQUIRED_TRANSPORT = {TransportSecurity.TLS_1_2, TransportSecurity.TLS_1_3}

    @staticmethod
    def validate_nist_800_175b(
        encryption: EncryptionStandard, transport: TransportSecurity, key_rotation_enabled: bool = True
    ) -> ValidationResult:
        """
        Validates configuration against NIST SP 800-175B (Guideline for Using Cryptographic Standards).
        """
        issues = []
        standards = []
        compliant = True

        # Check Encryption
        if encryption not in StorageCompliance.REQUIRED_ENCRYPTION:
            issues.append(
                f"Encryption {encryption.value} does not meet NIST SP 800-175B (requires AES-256-GCM or ChaCha20-Poly1305)"
            )
            compliant = False
        else:
            standards.append("NIST.SP.800-175B.Encryption")

        # Check Transport
        if transport not in StorageCompliance.REQUIRED_TRANSPORT:
            issues.append(f"Transport {transport.value} does not meet NIST requirements (requires TLS 1.2+)")
            compliant = False
        else:
            standards.append("NIST.SP.800-52r2.Transport")

        # Check Key Rotation
        if not key_rotation_enabled:
            issues.append("Key rotation is disabled (NIST requirement)")
            compliant = False
        else:
            standards.append("NIST.SP.800-57.KeyManagement")

        return ValidationResult(
            compliant=compliant,
            standards_met=standards,
            issues=issues,
            remediation_steps=["Enable AES-256-GCM", "Ensure TLS 1.2+", "Enable Key Rotation"] if not compliant else [],
        )

    @staticmethod
    def validate_iso_19772(auth_mechanisms: list[str], encryption: EncryptionStandard) -> ValidationResult:
        """
        Validates configuration against ISO/IEC 19772 (Authenticated Encryption).
        """
        issues = []
        standards = []
        compliant = True

        # ISO/IEC 19772 strictly requires Authenticated Encryption (AE)
        if encryption not in [EncryptionStandard.AES_256_GCM, EncryptionStandard.ChaCha20_Poly1305]:
            issues.append(
                f"Encryption {encryption.value} is not an approved Authenticated Encryption mechanism (ISO/IEC 19772)"
            )
            compliant = False
        else:
            standards.append("ISO/IEC.19772.AuthenticatedEncryption")

        # Check Authentication Context
        if not auth_mechanisms:
            issues.append("No authentication mechanism provided (ISO/IEC 19772 requirement)")
            compliant = False

        return ValidationResult(compliant=compliant, standards_met=standards, issues=issues)
