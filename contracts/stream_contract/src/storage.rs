//! Ledger storage helpers for streams and protocol configuration.
//!
//! This module is the single place where the contract touches its ledger
//! entries. Every access goes through a helper here so that two things stay
//! centralised: which [`DataKey`] backs a given piece of state, and how long that
//! state is kept alive on-chain.
//!
//! Rationale for keeping TTL policy in one file is recorded in
//! `docs/adr/0001-soroban-storage-ttl-bump-strategy.md` — reviewers should be
//! able to audit rent and eviction risk by reading this module alone.
//!
//! # Storage key layout
//!
//! All contract storage is keyed exclusively through [`DataKey`], declared in
//! `types.rs`. The variants used by this module, and where each one lives, are:
//!
//! | Variant | Storage class | Value | Written by | Read by |
//! |---|---|---|---|---|
//! | `StreamCounter` | Instance | `u64` | [`next_stream_id`] | [`next_stream_id`] |
//! | `Stream(u64)` | Persistent | [`Stream`] | [`save_stream`], [`remove_stream`] | [`load_stream`], [`try_load_stream`] |
//! | `ProtocolConfig` | Instance | [`ProtocolConfig`] | [`save_config`] | [`config_exists`], [`load_config`], [`try_load_config`] |
//! | `ContractVersion` | Instance | `u32` | [`save_contract_version`] | [`get_contract_version`] |
//! | `ContractWasmHash` | Instance | `BytesN<32>` | [`save_recorded_wasm_hash`] | [`get_recorded_wasm_hash`] |
//!
//! Instance storage holds singletons and small scalars, which keeps them
//! O(1) to address and avoids per-entry rent. Persistent storage is used only
//! for [`Stream`] records, the only entries whose size and lifetime scale with
//! usage. The enum itself is the single source of truth for the layout: adding a
//! variant means adding a row here and a helper that touches it.
//!
//! # TTL policy
//!
//! Soroban evicts an entry once its TTL runs out and charges rent per
//! persistent entry, so entries need to be renewed. Renewal goes through
//! `extend_ttl(threshold, bump_amount)`, which is a *conditional* operation: it
//! only extends when the remaining TTL has fallen below `threshold`, adding
//! `bump_amount` ledgers when it does. The two pairs of constants below are
//! therefore read as "renew once fewer than ~7 days remain, by ~30 days".
//!
//! Renewal is applied on **write** paths only. `next_stream_id`, `save_stream`
//! and `save_config` each renew; the read helpers and the two metadata setters
//! do not. In practice that is sufficient, because a stream that is being read
//! is by definition being operated on, and the write half of that same
//! invocation performs the bump.
//!
//! Note that ADR 0001, rule 1, describes *every* read/write path as bumping
//! TTL. The read paths documented below do not in fact call `extend_ttl`; the
//! ADR is currently more aspirational than the code. This module documents
//! observed behaviour — reconciling the two is a separate change, and
//! deliberately not one that was folded into a comments-only pass.

use soroban_sdk::{Env, Map, Symbol, TryFromVal, Val};

/// Minimum ledgers remaining before a persistent entry is renewed.
///
/// Applied as the `threshold` argument of
/// `extend_ttl` on `DataKey::Stream` entries. ~7 days at 5s per ledger; renewal
/// only happens when an entry has already aged past this point.
pub const PERSISTENT_LIFETIME_THRESHOLD: u32 = 120_960;
/// Number of ledgers added when renewing persistent storage.
///
/// Applied as the `bump_amount` argument of `extend_ttl`. ~30 days at 5s per
/// ledger, so a renewed stream entry survives roughly 30 further days of
/// inactivity.
pub const PERSISTENT_BUMP_AMOUNT: u32 = 518_400;
/// Minimum ledgers remaining before instance storage is renewed.
///
/// Applied as the `threshold` argument of `extend_ttl` on instance storage.
/// Matches [`PERSISTENT_LIFETIME_THRESHOLD`].
pub const INSTANCE_LIFETIME_THRESHOLD: u32 = 120_960;
/// Number of ledgers added when renewing instance storage.
///
/// Applied as the `bump_amount` argument of `extend_ttl` on instance storage.
/// Matches [`PERSISTENT_BUMP_AMOUNT`].
pub const INSTANCE_BUMP_AMOUNT: u32 = 518_400;

use crate::errors::StreamError;
use crate::types::{
    DataKey, DisputeStatus, LegacyProtocolConfig, LegacyStream, ProtocolConfig, Stream,
    VestingSchedule,
};

// ─── Version-Tolerant Decoding ────────────────────────────────────────────────

/// Field counts of the current and pre-v3 record shapes.
///
/// A `#[contracttype]` struct is stored as a host `Map` with one entry per field,
/// and decoding it walks the map positionally. The current shapes are described
/// here only so the two can be told apart before a decode is attempted.
const CONFIG_FIELD_COUNT: u32 = 5;
const LEGACY_CONFIG_FIELD_COUNT: u32 = 3;
const STREAM_FIELD_COUNT: u32 = 16;
const LEGACY_STREAM_FIELD_COUNT: u32 = 12;

/// Returns the number of fields in a stored record, or `None` if it is not a map.
///
/// Pure helper over an already-loaded value; it touches no storage of its own
/// and therefore performs no TTL bump.
///
/// This is the only safe way to distinguish the two record shapes. "Decode as
/// the current struct, and fall back to the legacy struct on `Err`" does **not**
/// work: a positional decode that runs off the end of a shorter map raises a
/// *host* error, which surfaces as a panic that aborts the invocation rather
/// than an `Err` the caller could branch on. Inspecting the map first means the
/// decode that does run is always the one that matches the stored bytes.
fn record_field_count(env: &Env, raw: &Val) -> Option<u32> {
    Map::<Symbol, Val>::try_from_val(env, raw)
        .ok()
        .map(|m| m.len())
}

// ─── Stream Counter ───────────────────────────────────────────────────────────

/// Returns the next stream ID and persists the updated counter.
///
/// Uses instance storage for the counter (O(1) access, singleton semantics).
/// IDs start at 1.
///
/// - **Key:** `DataKey::StreamCounter` in instance storage.
/// - **TTL bump:** Yes. The write is followed by
///   `extend_ttl(INSTANCE_LIFETIME_THRESHOLD, INSTANCE_BUMP_AMOUNT)`, which
///   renews the whole instance footprint. The bump happens on the instance
///   scope rather than per-key, so it also carries `DataKey::ProtocolConfig`,
///   `DataKey::ContractVersion` and `DataKey::ContractWasmHash` forward in time.
pub fn next_stream_id(env: &Env) -> u64 {
    let id: u64 = env
        .storage()
        .instance()
        .get(&DataKey::StreamCounter)
        .unwrap_or(0)
        + 1;
    env.storage().instance().set(&DataKey::StreamCounter, &id);
    env.storage()
        .instance()
        .extend_ttl(INSTANCE_LIFETIME_THRESHOLD, INSTANCE_BUMP_AMOUNT);
    id
}

// ─── Stream CRUD ─────────────────────────────────────────────────────────────

/// Loads a stream by ID from persistent storage, tolerating the legacy shape.
///
/// A pre-v2 record has no `schedule` field, so decoding it as the current
/// [`Stream`] fails. Rather than bricking escrowed funds after an in-place code
/// upgrade, fall back to [`LegacyStream`] and report it as the linear drip it
/// was created as. The upgraded record is not written back here — the next
/// `save_stream` for that ID persists the current shape, which is how
/// `migrate`'s lazy per-stream healing works.
///
/// Returns `StreamNotFound` if no entry exists in either shape.
///
/// - **Key:** reads `DataKey::Stream(stream_id)` from persistent storage.
/// - **TTL bump:** No. This is a read-only accessor; a passive read deliberately
///   does not pay the renewal write. Streams are renewed by the mutating
///   operations that follow a read.
pub fn load_stream(env: &Env, stream_id: u64) -> Result<Stream, StreamError> {
    try_load_stream(env, stream_id).ok_or(StreamError::StreamNotFound)
}

/// Persists a stream record in persistent storage.
///
/// Always use this instead of calling `.set` directly so that the key
/// strategy remains the single source of truth.
///
/// - **Key:** writes `DataKey::Stream(stream_id)` to persistent storage.
/// - **TTL bump:** Yes, scoped to this stream's key only — the bump targets
///   `&key` rather than the whole footprint, so a write never extends the rent
///   or TTL of unrelated streams. Amounts are
///   [`PERSISTENT_LIFETIME_THRESHOLD`] / [`PERSISTENT_BUMP_AMOUNT`].
pub fn save_stream(env: &Env, stream_id: u64, stream: &Stream) {
    let key = DataKey::Stream(stream_id);
    env.storage().persistent().set(&key, stream);
    env.storage().persistent().extend_ttl(
        &key,
        PERSISTENT_LIFETIME_THRESHOLD,
        PERSISTENT_BUMP_AMOUNT,
    );
}

/// Returns the stream if it exists, `None` otherwise (used by read-only queries).
///
/// - **Key:** reads `DataKey::Stream(stream_id)` from persistent storage.
/// - **TTL bump:** No. See [`load_stream`]; renewal happens on the write path.
pub fn try_load_stream(env: &Env, stream_id: u64) -> Option<Stream> {
    let raw: Option<Val> = env.storage().persistent().get(&DataKey::Stream(stream_id));

    // Reading as a bare `Val` is what makes the legacy fallback possible:
    // `storage.get::<_, Stream>` collapses "absent" and "undecodable" into the
    // same `None`, so the value is inspected before anything is decoded.
    let raw = raw?;

    match record_field_count(env, &raw)? {
        STREAM_FIELD_COUNT => Stream::try_from_val(env, &raw).ok(),
        LEGACY_STREAM_FIELD_COUNT => LegacyStream::try_from_val(env, &raw)
            .ok()
            .map(upgrade_legacy_stream),
        // An unknown shape is a corrupt record, not a legacy one. Reporting it
        // as "missing" would look like an empty stream to every caller.
        _ => None,
    }
}

/// Widens a legacy stream record to the current shape.
///
/// Pure in-memory transform; touches no storage and performs no TTL bump.
fn upgrade_legacy_stream(legacy: LegacyStream) -> Stream {
    Stream {
        sender: legacy.sender,
        recipient: legacy.recipient,
        token_address: legacy.token_address,
        rate_per_second: legacy.rate_per_second,
        deposited_amount: legacy.deposited_amount,
        withdrawn_amount: legacy.withdrawn_amount,
        start_time: legacy.start_time,
        last_update_time: legacy.last_update_time,
        is_active: legacy.is_active,
        paused: legacy.paused,
        paused_at: legacy.paused_at,
        status: legacy.status,
        // A stream with no schedule field predates step vesting: it is a
        // continuous drip by construction.
        schedule: VestingSchedule::Linear,
        // New fields default to no arbiter, no dispute, and non-allowance-based.
        arbiter: None,
        dispute_status: DisputeStatus::None,
        is_allowance_based: false,
    }
}

// ─── Protocol Config ──────────────────────────────────────────────────────────

/// Checks whether the protocol config has already been initialized.
///
/// - **Key:** probes `DataKey::ProtocolConfig` in instance storage via `has`.
/// - **TTL bump:** No. `has` is a membership test that loads no value.
pub fn config_exists(env: &Env) -> bool {
    env.storage().instance().has(&DataKey::ProtocolConfig)
}

/// Loads the protocol config, transparently upgrading a pre-v2 record in memory.
///
/// An older deployment persisted a three-field [`LegacyProtocolConfig`]. A
/// `#[contracttype]` struct decodes field-by-field from a Soroban `Map`, so
/// reading the five-field [`ProtocolConfig`] out of a legacy record does not
/// fail cleanly — see [`record_field_count`]. Rather than bricking the contract
/// after an in-place code upgrade, the record's field count selects the legacy
/// shape and reports it with the safe defaults `is_protocol_paused: false` and
/// `emergency_guardian: None`.
///
/// The upgraded value is *not* written back here — `load_config` is read-only.
/// [`crate::StreamContract::migrate`] performs the actual persisted upgrade.
///
/// # Errors
/// - `NotInitialized` — no config present in either shape.
///
/// - **Key:** reads `DataKey::ProtocolConfig` from instance storage.
/// - **TTL bump:** No, by design — the legacy upgrade applied here is in-memory
///   only, so persisting it (and paying the write) is left to `migrate`.
pub fn load_config(env: &Env) -> Result<ProtocolConfig, StreamError> {
    try_load_config(env).ok_or(StreamError::NotInitialized)
}

/// Reads the protocol config as an `Option`, tolerating the legacy shape.
///
/// Used both by the mandatory load path and by optional fee-collection logic.
///
/// - **Key:** reads `DataKey::ProtocolConfig` from instance storage.
/// - **TTL bump:** No. See [`load_config`].
pub fn try_load_config(env: &Env) -> Option<ProtocolConfig> {
    let raw: Option<Val> = env.storage().instance().get(&DataKey::ProtocolConfig);

    // See `record_field_count`: the shape must be known before a decode is
    // attempted, so a legacy record is never fed to the wider current struct.
    let raw = raw?;

    match record_field_count(env, &raw)? {
        CONFIG_FIELD_COUNT => ProtocolConfig::try_from_val(env, &raw).ok(),
        LEGACY_CONFIG_FIELD_COUNT => {
            LegacyProtocolConfig::try_from_val(env, &raw)
                .ok()
                .map(|legacy| ProtocolConfig {
                    admin: legacy.admin,
                    treasury: legacy.treasury,
                    fee_rate_bps: legacy.fee_rate_bps,
                    is_protocol_paused: false,
                    emergency_guardian: None,
                })
        }
        _ => None,
    }
}

/// Persists the protocol config.
///
/// - **Key:** writes `DataKey::ProtocolConfig` to instance storage.
/// - **TTL bump:** Yes, at instance scope, with
///   [`INSTANCE_LIFETIME_THRESHOLD`] / [`INSTANCE_BUMP_AMOUNT`]. The bump is
///   not per-key, so it renews the entire instance footprint along with this
///   write.
pub fn save_config(env: &Env, config: &ProtocolConfig) {
    env.storage()
        .instance()
        .set(&DataKey::ProtocolConfig, config);
    env.storage()
        .instance()
        .extend_ttl(INSTANCE_LIFETIME_THRESHOLD, INSTANCE_BUMP_AMOUNT);
}

// ─── State Schema Versioning ──────────────────────────────────────────────────

/// Reads the persisted state schema version.
///
/// `0` means the state was written before versioning existed and still uses the
/// legacy layout.
///
/// - **Key:** reads `DataKey::ContractVersion` from instance storage, falling
///   back to `0` when absent.
/// - **TTL bump:** No. Absent value is treated as a legacy `0` rather than
///   written, so this stays side-effect free.
pub fn get_contract_version(env: &Env) -> u32 {
    env.storage()
        .instance()
        .get::<DataKey, u32>(&DataKey::ContractVersion)
        .unwrap_or(0)
}

/// Persists the state schema version.
///
/// - **Key:** writes `DataKey::ContractVersion` to instance storage.
/// - **TTL bump:** No. This is deliberately a bare write with no `extend_ttl`:
///   the value is small, is rewritten by `migrate` and `upgrade`, and inherits
///   renewal from the instance-scope bumps in [`next_stream_id`] and
///   [`save_config`].
pub fn save_contract_version(env: &Env, version: u32) {
    env.storage()
        .instance()
        .set(&DataKey::ContractVersion, &version);
}

// ─── Executable Hash Tracking ─────────────────────────────────────────────────

/// Reads the executable hash recorded by the most recent `upgrade`.
///
/// Returns `BytesN::zero` when the contract has never been upgraded in place,
/// since the host offers no way to read the live executable.
///
/// - **Key:** reads `DataKey::ContractWasmHash` from instance storage, defaulting
///   to `BytesN::zero` when absent.
/// - **TTL bump:** No.
pub fn get_recorded_wasm_hash(env: &Env) -> soroban_sdk::BytesN<32> {
    env.storage()
        .instance()
        .get::<DataKey, soroban_sdk::BytesN<32>>(&DataKey::ContractWasmHash)
        .unwrap_or_else(|| soroban_sdk::BytesN::from_array(env, &[0u8; 32]))
}

/// Records the executable hash installed by an `upgrade`.
///
/// - **Key:** writes `DataKey::ContractWasmHash` to instance storage.
/// - **TTL bump:** No. See [`save_contract_version`]; the value rides on the
///   instance-scope renewal performed by the surrounding admin operation.
pub fn save_recorded_wasm_hash(env: &Env, hash: &soroban_sdk::BytesN<32>) {
    env.storage()
        .instance()
        .set(&DataKey::ContractWasmHash, hash);
}

// ─── Stream Deletion ──────────────────────────────────────────────────────────

/// Removes a stream record from persistent storage.
///
/// Used to prune fully settled streams and reclaim storage rent.
///
/// - **Key:** removes `DataKey::Stream(stream_id)` from persistent storage.
/// - **TTL bump:** None, and none is possible: the entry is being deleted. The
///   caller's transaction fee is what pays for reclaiming the rent, and
///   removing the entry is the point — bumping a stream that is about to be
///   pruned would only waste keeper gas.
pub fn remove_stream(env: &Env, stream_id: u64) {
    let key = DataKey::Stream(stream_id);
    env.storage().persistent().remove(&key);
}
