use std::collections::HashSet;
use std::str::FromStr;

use alloy_primitives::{Address, Signature};
use clap::ValueEnum;
use tonic::Status;

/// How the gateway validates incoming signed requests.
///
/// Applies only to RPCs that carry a signed body (`create_artifact`,
/// `create_program`, `request_proof`). Read-only RPCs are unauthenticated.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum AuthMode {
    /// Accept all requests; `requester` is reported as the zero address.
    #[default]
    None,
    /// Verify the signature and set `requester` to the recovered address.
    Verify,
    /// `Verify` + require the recovered address to be in the allowlist.
    Allowlist,
}

#[derive(Clone, Debug, Default)]
pub struct Auth {
    pub mode: AuthMode,
    pub allowlist: HashSet<Address>,
}

impl Auth {
    /// Verify a signature over `message` and return the requester address.
    ///
    /// - `None` → always returns the zero address; signature untouched.
    /// - `Verify`/`Allowlist` → parses the signature as 65-byte `[r||s||v]`, recovers the
    ///   signer via EIP-191, and (for `Allowlist`) checks membership.
    #[allow(clippy::result_large_err)] // matches tonic's Status return shape
    pub fn authorize(&self, message: &[u8], signature: &[u8]) -> Result<Address, Status> {
        match self.mode {
            AuthMode::None => Ok(Address::ZERO),
            AuthMode::Verify | AuthMode::Allowlist => {
                let sig = Signature::from_raw(signature).map_err(|e| {
                    Status::unauthenticated(format!("invalid signature bytes: {e}"))
                })?;
                let addr = sig.recover_address_from_msg(message).map_err(|e| {
                    Status::unauthenticated(format!("signature recovery failed: {e}"))
                })?;
                if self.mode == AuthMode::Allowlist && !self.allowlist.contains(&addr) {
                    return Err(Status::permission_denied(format!(
                        "signer {addr} not in allowlist"
                    )));
                }
                Ok(addr)
            }
        }
    }

    /// [`Self::authorize`] for `create_artifact`, whose signature the SP1 SDK
    /// builds differently from every other signed request.
    ///
    /// sp1-sdk's `network::utils::sign_message` (still in 6.10.0) takes
    /// `Signature::as_bytes`, whose v is already in Electrum form (27/28), and
    /// adds 27 again, so v arrives as 54/55. alloy reads any v >= 35 as EIP-155
    /// and gets the parity backwards, so recovery yields a wrong address: the
    /// same one every time for a local key (deterministic RFC 6979 signatures
    /// over this fixed message), a different one per call for a KMS key
    /// (randomized signatures). `Allowlist` then rejects the client and
    /// `Verify` reports the wrong requester. Map 54/55 back to 27/28 here;
    /// standard v values pass through. EIP-155 v has no meaning for a signed
    /// message, so nothing valid is reinterpreted.
    #[allow(clippy::result_large_err)] // matches tonic's Status return shape
    pub fn authorize_create_artifact(
        &self,
        message: &[u8],
        signature: &[u8],
    ) -> Result<Address, Status> {
        match signature {
            [rs @ .., v @ (54 | 55)] if signature.len() == 65 => {
                let mut fixed = [0u8; 65];
                fixed[..64].copy_from_slice(rs);
                fixed[64] = v - 27;
                self.authorize(message, &fixed)
            }
            _ => self.authorize(message, signature),
        }
    }
}

/// Parse a comma-separated list of 0x-prefixed addresses.
pub fn parse_allowlist(input: &str) -> Result<HashSet<Address>, String> {
    input
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| Address::from_str(s).map_err(|e| format!("invalid address {s}: {e}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    fn sign(message: &[u8], signer: &PrivateKeySigner) -> Vec<u8> {
        signer
            .sign_message_sync(message)
            .unwrap()
            .as_bytes()
            .to_vec()
    }

    /// A `create_artifact` signature exactly as sp1-sdk 6.10.0 (unchanged since 6.8.1) builds it:
    /// `as_bytes()` (v = 27/28), then `v + 27`.
    fn sdk_create_artifact_signature(signer: &PrivateKeySigner) -> Vec<u8> {
        let mut sig = sign(b"create_artifact", signer);
        sig[64] += 27;
        sig
    }

    /// Pins down why the remap exists: alloy misreads v = 54/55. The signature
    /// here is built by this test, so it cannot tell whether the SDK still adds
    /// the extra 27; on SDK upgrades, re-check `network::utils::sign_message`.
    #[test]
    fn generic_authorize_misreads_the_sdk_create_artifact_signature() {
        let auth = Auth {
            mode: AuthMode::Verify,
            allowlist: Default::default(),
        };
        let signer = PrivateKeySigner::random();
        let sig = sdk_create_artifact_signature(&signer);
        assert!(matches!(sig[64], 54 | 55));
        assert_ne!(
            auth.authorize(b"create_artifact", &sig).unwrap(),
            signer.address()
        );
    }

    #[test]
    fn create_artifact_recovers_the_sdk_signer() {
        let auth = Auth {
            mode: AuthMode::Verify,
            allowlist: Default::default(),
        };
        // Random keys until both v = 54 and v = 55 have been checked.
        let mut seen = HashSet::new();
        while seen.len() < 2 {
            let signer = PrivateKeySigner::random();
            let sig = sdk_create_artifact_signature(&signer);
            assert_eq!(
                auth.authorize_create_artifact(b"create_artifact", &sig)
                    .unwrap(),
                signer.address(),
                "v = {}",
                sig[64]
            );
            seen.insert(sig[64]);
        }
        assert_eq!(seen, HashSet::from([54, 55]));
    }

    #[test]
    fn create_artifact_still_accepts_a_standard_signature() {
        let auth = Auth {
            mode: AuthMode::Verify,
            allowlist: Default::default(),
        };
        let signer = PrivateKeySigner::random();
        let sig = sign(b"create_artifact", &signer);
        assert_eq!(
            auth.authorize_create_artifact(b"create_artifact", &sig)
                .unwrap(),
            signer.address()
        );
    }

    #[test]
    fn none_mode_returns_zero_address() {
        let auth = Auth {
            mode: AuthMode::None,
            allowlist: Default::default(),
        };
        let addr = auth.authorize(b"anything", b"garbage").unwrap();
        assert_eq!(addr, Address::ZERO);
    }

    #[test]
    fn verify_recovers_signer() {
        let signer = PrivateKeySigner::random();
        let expected = signer.address();
        let msg = b"hello gateway";
        let sig = sign(msg, &signer);

        let auth = Auth {
            mode: AuthMode::Verify,
            allowlist: Default::default(),
        };
        assert_eq!(auth.authorize(msg, &sig).unwrap(), expected);
    }

    #[test]
    fn verify_rejects_wrong_signature() {
        let msg = b"hello";
        let auth = Auth {
            mode: AuthMode::Verify,
            allowlist: Default::default(),
        };
        let err = auth.authorize(msg, &[0u8; 65]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn allowlist_admits_members_and_rejects_others() {
        let insider = PrivateKeySigner::random();
        let outsider = PrivateKeySigner::random();
        let msg = b"payload";

        let allowlist: HashSet<_> = std::iter::once(insider.address()).collect();
        let auth = Auth {
            mode: AuthMode::Allowlist,
            allowlist,
        };

        assert_eq!(
            auth.authorize(msg, &sign(msg, &insider)).unwrap(),
            insider.address()
        );
        let err = auth.authorize(msg, &sign(msg, &outsider)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn parse_allowlist_handles_mixed_whitespace() {
        let parsed = parse_allowlist(
            "0x0000000000000000000000000000000000000001, 0x0000000000000000000000000000000000000002",
        )
        .unwrap();
        assert_eq!(parsed.len(), 2);
        assert!(parsed
            .contains(&Address::from_str("0x0000000000000000000000000000000000000001").unwrap()));
    }
}
