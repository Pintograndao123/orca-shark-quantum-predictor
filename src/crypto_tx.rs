use std::convert::TryInto;
use std::fs;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct U256(pub [u64; 4]); // little-endian limbs: [l0, l1, l2, l3]

impl U256 {
    pub const ZERO: Self = Self([0, 0, 0, 0]);
    pub const ONE: Self = Self([1, 0, 0, 0]);

    // secp256k1 field prime P = 2^256 - 2^32 - 977
    pub const P: Self = Self([
        0xFFFFFFFEFFFFFC2F,
        0xFFFFFFFFFFFFFFFF,
        0xFFFFFFFFFFFFFFFF,
        0xFFFFFFFFFFFFFFFF,
    ]);

    // secp256k1 curve order N
    pub const N: Self = Self([
        0xBFD25E8CD0364141,
        0xBAAEDCE6AF48A03B,
        0xFFFFFFFFFFFFFFFE,
        0xFFFFFFFFFFFFFFFF,
    ]);

    // N / 2 for EIP-2 low-S signature canonicalization
    pub const HALF_N: Self = Self([
        0xDFE92F46681B20A0,
        0x5D576E7357A4501D,
        0xFFFFFFFFFFFFFFFF,
        0x7FFFFFFFFFFFFFFF,
    ]);

    pub fn from_be_bytes(b: &[u8; 32]) -> Self {
        let l3 = u64::from_be_bytes(b[0..8].try_into().unwrap());
        let l2 = u64::from_be_bytes(b[8..16].try_into().unwrap());
        let l1 = u64::from_be_bytes(b[16..24].try_into().unwrap());
        let l0 = u64::from_be_bytes(b[24..32].try_into().unwrap());
        Self([l0, l1, l2, l3])
    }

    pub fn from_be_hex(h: &str) -> Option<Self> {
        let h = h.trim().trim_start_matches("0x").trim_start_matches("0X");
        if h.len() > 64 || h.is_empty() || !h.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        let padded = format!("{:0>64}", h);
        let l3 = u64::from_str_radix(&padded[0..16], 16).ok()?;
        let l2 = u64::from_str_radix(&padded[16..32], 16).ok()?;
        let l1 = u64::from_str_radix(&padded[32..48], 16).ok()?;
        let l0 = u64::from_str_radix(&padded[48..64], 16).ok()?;
        Some(Self([l0, l1, l2, l3]))
    }

    pub fn from_u128(v: u128) -> Self {
        Self([v as u64, (v >> 64) as u64, 0, 0])
    }

    pub fn to_be_bytes(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        for i in 0..4 {
            out[(3 - i) * 8..(4 - i) * 8].copy_from_slice(&self.0[i].to_be_bytes());
        }
        out
    }

    pub fn to_minimal_be_bytes(self) -> Vec<u8> {
        let b = self.to_be_bytes();
        let first_nz = b.iter().position(|&x| x != 0).unwrap_or(32);
        b[first_nz..].to_vec()
    }

    pub fn ge(self, other: Self) -> bool {
        for i in (0..4).rev() {
            if self.0[i] != other.0[i] {
                return self.0[i] > other.0[i];
            }
        }
        true
    }

    pub fn gt(self, other: Self) -> bool {
        self != other && self.ge(other)
    }

    pub fn sub_raw(self, other: Self) -> (Self, bool) {
        let mut r = [0u64; 4];
        let mut borrow = 0u64;
        for i in 0..4 {
            let (d1, b1) = self.0[i].overflowing_sub(other.0[i]);
            let (d2, b2) = d1.overflowing_sub(borrow);
            r[i] = d2;
            borrow = (b1 as u64) + (b2 as u64);
        }
        (Self(r), borrow != 0)
    }

    pub fn add_mod_m(self, other: Self, m: Self) -> Self {
        let a = if self.ge(m) { self.sub_raw(m).0 } else { self };
        let b = if other.ge(m) { other.sub_raw(m).0 } else { other };
        let mut r = [0u64; 4];
        let mut carry = 0u64;
        for i in 0..4 {
            let (s1, c1) = a.0[i].overflowing_add(b.0[i]);
            let (s2, c2) = s1.overflowing_add(carry);
            r[i] = s2;
            carry = (c1 as u64) + (c2 as u64);
        }
        let res = Self(r);
        if carry != 0 || res.ge(m) {
            res.sub_raw(m).0
        } else {
            res
        }
    }

    pub fn sub_mod_m(self, other: Self, m: Self) -> Self {
        let a = if self.ge(m) { self.sub_raw(m).0 } else { self };
        let b = if other.ge(m) { other.sub_raw(m).0 } else { other };
        let (d, borrow) = a.sub_raw(b);
        if borrow {
            let mut r = [0u64; 4];
            let mut carry = 0u64;
            for i in 0..4 {
                let (s1, c1) = d.0[i].overflowing_add(m.0[i]);
                let (s2, c2) = s1.overflowing_add(carry);
                r[i] = s2;
                carry = (c1 as u64) + (c2 as u64);
            }
            Self(r)
        } else {
            d
        }
    }

    pub fn mul_mod_m(self, other: Self, m: Self) -> Self {
        let mut res = Self::ZERO;
        let mut base = if self.ge(m) { self.sub_raw(m).0 } else { self };
        for limb in other.0 {
            for bit in 0..64 {
                if (limb >> bit) & 1 == 1 {
                    res = res.add_mod_m(base, m);
                }
                base = base.add_mod_m(base, m);
            }
        }
        res
    }

    pub fn pow_mod_m(self, exp: Self, m: Self) -> Self {
        let mut res = Self::ONE;
        let mut base = if self.ge(m) { self.sub_raw(m).0 } else { self };
        for limb in exp.0 {
            for bit in 0..64 {
                if (limb >> bit) & 1 == 1 {
                    res = res.mul_mod_m(base, m);
                }
                base = base.mul_mod_m(base, m);
            }
        }
        res
    }

    pub fn inv_mod_m(self, m: Self) -> Self {
        let m_minus_2 = m.sub_raw(Self([2, 0, 0, 0])).0;
        self.pow_mod_m(m_minus_2, m)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Point {
    pub x: U256,
    pub y: U256,
    pub inf: bool,
}

impl Point {
    pub const INF: Self = Self {
        x: U256::ZERO,
        y: U256::ZERO,
        inf: true,
    };

    pub fn generator() -> Self {
        Self {
            x: U256::from_be_hex("79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798").unwrap(),
            y: U256::from_be_hex("483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8").unwrap(),
            inf: false,
        }
    }

    pub fn double(self) -> Self {
        if self.inf || self.y == U256::ZERO {
            return Self::INF;
        }
        let p = U256::P;
        let num = U256([3, 0, 0, 0]).mul_mod_m(self.x, p).mul_mod_m(self.x, p);
        let den = U256([2, 0, 0, 0]).mul_mod_m(self.y, p).inv_mod_m(p);
        let lam = num.mul_mod_m(den, p);
        let x3 = lam.mul_mod_m(lam, p).sub_mod_m(self.x, p).sub_mod_m(self.x, p);
        let y3 = lam.mul_mod_m(self.x.sub_mod_m(x3, p), p).sub_mod_m(self.y, p);
        Self { x: x3, y: y3, inf: false }
    }

    pub fn add(self, q: Self) -> Self {
        if self.inf {
            return q;
        }
        if q.inf {
            return self;
        }
        let p = U256::P;
        if self.x == q.x {
            if self.y == q.y {
                return self.double();
            } else {
                return Self::INF;
            }
        }
        let lam = q.y.sub_mod_m(self.y, p).mul_mod_m(q.x.sub_mod_m(self.x, p).inv_mod_m(p), p);
        let x3 = lam.mul_mod_m(lam, p).sub_mod_m(self.x, p).sub_mod_m(q.x, p);
        let y3 = lam.mul_mod_m(self.x.sub_mod_m(x3, p), p).sub_mod_m(self.y, p);
        Self { x: x3, y: y3, inf: false }
    }

    pub fn scalar_mul(self, k: U256) -> Self {
        let mut res = Self::INF;
        let mut base = self;
        for limb in k.0 {
            for bit in 0..64 {
                if (limb >> bit) & 1 == 1 {
                    res = res.add(base);
                }
                base = base.double();
            }
        }
        res
    }
}

pub fn keccak256(data: &[u8]) -> [u8; 32] {
    const RC: [u64; 24] = [
        0x0000000000000001, 0x0000000000008082, 0x800000000000808a, 0x8000000080008000,
        0x000000000000808b, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
        0x000000000000008a, 0x0000000000000088, 0x0000000080008009, 0x000000008000000a,
        0x000000008000808b, 0x800000000000008b, 0x8000000000008089, 0x8000000000008003,
        0x8000000000008002, 0x8000000000000080, 0x000000000000800a, 0x800000008000000a,
        0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
    ];
    const ROTC: [u32; 24] = [
        1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14, 27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44,
    ];
    const PILN: [usize; 24] = [
        10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4, 15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1,
    ];
    let rate = 136;
    let mut padded = data.to_vec();
    padded.push(0x01);
    while padded.len() % rate != 0 {
        padded.push(0x00);
    }
    let last = padded.len() - 1;
    padded[last] |= 0x80;

    let mut st = [0u64; 25];
    for chunk in padded.chunks(rate) {
        for i in 0..(rate / 8) {
            let mut b = [0u8; 8];
            b.copy_from_slice(&chunk[i * 8..(i + 1) * 8]);
            st[i] ^= u64::from_le_bytes(b);
        }
        for round in 0..24 {
            let mut bc = [0u64; 5];
            for i in 0..5 {
                bc[i] = st[i] ^ st[i + 5] ^ st[i + 10] ^ st[i + 15] ^ st[i + 20];
            }
            for i in 0..5 {
                let t = bc[(i + 4) % 5] ^ bc[(i + 1) % 5].rotate_left(1);
                for j in (0..25).step_by(5) {
                    st[j + i] ^= t;
                }
            }
            let mut t = st[1];
            for i in 0..24 {
                let j = PILN[i];
                let tmp = st[j];
                st[j] = t.rotate_left(ROTC[i]);
                t = tmp;
            }
            for j in (0..25).step_by(5) {
                let row = [st[j], st[j + 1], st[j + 2], st[j + 3], st[j + 4]];
                for i in 0..5 {
                    st[j + i] ^= (!row[(i + 1) % 5]) & row[(i + 2) % 5];
                }
            }
            st[0] ^= RC[round];
        }
    }
    let mut out = [0u8; 32];
    for i in 0..4 {
        out[i * 8..(i + 1) * 8].copy_from_slice(&st[i].to_le_bytes());
    }
    out
}

pub fn priv_to_addr_bytes(priv_key: U256) -> Option<[u8; 20]> {
    if priv_key == U256::ZERO || priv_key.ge(U256::N) {
        return None;
    }
    let pub_pt = Point::generator().scalar_mul(priv_key);
    if pub_pt.inf {
        return None;
    }
    let mut pub64 = [0u8; 64];
    pub64[0..32].copy_from_slice(&pub_pt.x.to_be_bytes());
    pub64[32..64].copy_from_slice(&pub_pt.y.to_be_bytes());
    let h = keccak256(&pub64);
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&h[12..32]);
    Some(addr)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EcdsaSig {
    pub r: U256,
    pub s: U256,
    pub y_parity: u8,
}

pub fn sign_hash_deterministic(priv_key: U256, msg_hash: &[u8; 32]) -> Option<EcdsaSig> {
    if priv_key == U256::ZERO || priv_key.ge(U256::N) {
        return None;
    }
    let n = U256::N;
    let z_raw = U256::from_be_bytes(msg_hash);
    let z = if z_raw.ge(n) { z_raw.sub_raw(n).0 } else { z_raw };
    let d_bytes = priv_key.to_be_bytes();

    for counter in 0u32..1024 {
        let mut seed = Vec::with_capacity(96);
        seed.extend_from_slice(b"POLYGON_FLASH_SNIPER_DET_ECDSA_V1");
        seed.extend_from_slice(&d_bytes);
        seed.extend_from_slice(msg_hash);
        seed.extend_from_slice(&counter.to_be_bytes());
        let k_hash = keccak256(&seed);
        let k_raw = U256::from_be_bytes(&k_hash);
        let k = if k_raw.ge(n) { k_raw.sub_raw(n).0 } else { k_raw };
        if k == U256::ZERO {
            continue;
        }
        let r_pt = Point::generator().scalar_mul(k);
        if r_pt.inf {
            continue;
        }
        let mut y_parity = (r_pt.y.0[0] & 1) as u8;
        let r = if r_pt.x.ge(n) {
            y_parity |= 2;
            r_pt.x.sub_raw(n).0
        } else {
            r_pt.x
        };
        if r == U256::ZERO {
            continue;
        }
        let k_inv = k.inv_mod_m(n);
        let rd = r.mul_mod_m(priv_key, n);
        let mut s = k_inv.mul_mod_m(z.add_mod_m(rd, n), n);
        if s == U256::ZERO {
            continue;
        }
        // EIP-2 low-S canonicalization
        if s.gt(U256::HALF_N) {
            s = n.sub_raw(s).0;
            y_parity ^= 1;
        }
        return Some(EcdsaSig { r, s, y_parity });
    }
    None
}

// RLP encoding primitives
pub fn rlp_encode_bytes(data: &[u8]) -> Vec<u8> {
    if data.len() == 1 && data[0] < 0x80 {
        vec![data[0]]
    } else if data.len() <= 55 {
        let mut out = Vec::with_capacity(1 + data.len());
        out.push(0x80 + data.len() as u8);
        out.extend_from_slice(data);
        out
    } else {
        let len_bytes = u64_minimal_be(data.len() as u64);
        let mut out = Vec::with_capacity(1 + len_bytes.len() + data.len());
        out.push(0xb7 + len_bytes.len() as u8);
        out.extend_from_slice(&len_bytes);
        out.extend_from_slice(data);
        out
    }
}

pub fn u64_minimal_be(v: u64) -> Vec<u8> {
    if v == 0 {
        return Vec::new();
    }
    let b = v.to_be_bytes();
    let idx = b.iter().position(|&x| x != 0).unwrap_or(8);
    b[idx..].to_vec()
}

pub fn u128_minimal_be(v: u128) -> Vec<u8> {
    if v == 0 {
        return Vec::new();
    }
    let b = v.to_be_bytes();
    let idx = b.iter().position(|&x| x != 0).unwrap_or(16);
    b[idx..].to_vec()
}

pub fn rlp_encode_u64(v: u64) -> Vec<u8> {
    rlp_encode_bytes(&u64_minimal_be(v))
}

pub fn rlp_encode_u128(v: u128) -> Vec<u8> {
    rlp_encode_bytes(&u128_minimal_be(v))
}

pub fn rlp_encode_u256(v: U256) -> Vec<u8> {
    rlp_encode_bytes(&v.to_minimal_be_bytes())
}

pub fn rlp_encode_list(items: &[Vec<u8>]) -> Vec<u8> {
    let payload_len: usize = items.iter().map(|i| i.len()).sum();
    let mut out = Vec::with_capacity(payload_len + 9);
    if payload_len <= 55 {
        out.push(0xc0 + payload_len as u8);
    } else {
        let lb = u64_minimal_be(payload_len as u64);
        out.push(0xf7 + lb.len() as u8);
        out.extend_from_slice(&lb);
    }
    for item in items {
        out.extend_from_slice(item);
    }
    out
}

pub struct Eip1559Tx {
    pub chain_id: u64,
    pub nonce: u64,
    pub max_priority_fee_per_gas: u128,
    pub max_fee_per_gas: u128,
    pub gas_limit: u64,
    pub to: Option<[u8; 20]>, // None for CREATE deployment
    pub value: u128,
    pub data: Vec<u8>,
}

impl Eip1559Tx {
    pub fn sign(&self, priv_key: U256) -> Option<(Vec<u8>, [u8; 32])> {
        let to_bytes: Vec<u8> = match self.to {
            Some(addr) => rlp_encode_bytes(&addr),
            None => rlp_encode_bytes(&[]),
        };
        let empty_access_list = rlp_encode_list(&[]);

        let unsigned_items = vec![
            rlp_encode_u64(self.chain_id),
            rlp_encode_u64(self.nonce),
            rlp_encode_u128(self.max_priority_fee_per_gas),
            rlp_encode_u128(self.max_fee_per_gas),
            rlp_encode_u64(self.gas_limit),
            to_bytes.clone(),
            rlp_encode_u128(self.value),
            rlp_encode_bytes(&self.data),
            empty_access_list.clone(),
        ];
        let mut signing_payload = Vec::with_capacity(256);
        signing_payload.push(0x02);
        signing_payload.extend_from_slice(&rlp_encode_list(&unsigned_items));
        let sighash = keccak256(&signing_payload);
        let sig = sign_hash_deterministic(priv_key, &sighash)?;

        let signed_items = vec![
            rlp_encode_u64(self.chain_id),
            rlp_encode_u64(self.nonce),
            rlp_encode_u128(self.max_priority_fee_per_gas),
            rlp_encode_u128(self.max_fee_per_gas),
            rlp_encode_u64(self.gas_limit),
            to_bytes,
            rlp_encode_u128(self.value),
            rlp_encode_bytes(&self.data),
            empty_access_list,
            rlp_encode_u64(sig.y_parity as u64),
            rlp_encode_u256(sig.r),
            rlp_encode_u256(sig.s),
        ];
        let mut raw_tx = Vec::with_capacity(384);
        raw_tx.push(0x02);
        raw_tx.extend_from_slice(&rlp_encode_list(&signed_items));
        let tx_hash = keccak256(&raw_tx);
        Some((raw_tx, tx_hash))
    }
}

pub fn predict_create_address(sender: [u8; 20], nonce: u64) -> [u8; 20] {
    let encoded = rlp_encode_list(&[rlp_encode_bytes(&sender), rlp_encode_u64(nonce)]);
    let h = keccak256(&encoded);
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&h[12..32]);
    addr
}

pub fn predict_create2_address(factory: [u8; 20], salt: [u8; 32], initcode: &[u8]) -> [u8; 20] {
    let init_hash = keccak256(initcode);
    let mut pre = Vec::with_capacity(85);
    pre.push(0xff);
    pre.extend_from_slice(&factory);
    pre.extend_from_slice(&salt);
    pre.extend_from_slice(&init_hash);
    let h = keccak256(&pre);
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&h[12..32]);
    addr
}

pub fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

pub fn hex_to_bytes(h: &str) -> Option<Vec<u8>> {
    let clean = h.trim().trim_start_matches("0x").trim_start_matches("0X");
    if clean.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(clean.len() / 2);
    for i in (0..clean.len()).step_by(2) {
        out.push(u8::from_str_radix(&clean[i..i + 2], 16).ok()?);
    }
    Some(out)
}

pub fn parse_addr20(h: &str) -> Option<[u8; 20]> {
    let b = hex_to_bytes(h)?;
    if b.len() != 20 {
        return None;
    }
    let mut a = [0u8; 20];
    a.copy_from_slice(&b);
    Some(a)
}

/// Securely loads PK1 (EOA) and PK2 (Magic Signer) in RAM from .env without ever printing secrets.
pub struct VaultSigners {
    pub pk1: U256,
    pub pk1_addr: [u8; 20],
    pub pk2: Option<(U256, [u8; 20])>,
}

#[derive(Clone, Debug)]
pub struct PolymarketConfig {
    pub proxy_wallet: String,
    pub magic_signer: String,
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    pub proxies: Vec<String>,
}

fn search_env_candidates() -> Option<String> {
    let mut candidates = vec![
        std::env::var("VAULT_ENV_PATH").unwrap_or_default(),
        ".env".to_string(),
        "config/.env".to_string(),
        "../.env".to_string(),
        "../../.env".to_string(),
    ];
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            candidates.push(format!("{}/.env", parent.display()));
            candidates.push(format!("{}/config/.env", parent.display()));
            if let Some(gp) = parent.parent() {
                candidates.push(format!("{}/.env", gp.display()));
                if let Some(ggp) = gp.parent() {
                    candidates.push(format!("{}/.env", ggp.display()));
                }
            }
        }
    }

    for path in &candidates {
        if !path.is_empty() {
            if let Ok(c) = fs::read_to_string(path) {
                return Some(c);
            }
        }
    }
    None
}

pub fn load_vault_signers() -> Result<VaultSigners, String> {
    let mut pk1_hex = std::env::var("POLYMARKET_KEY").ok();
    let mut pk2_hex = std::env::var("PK2_KEY").ok();

    if let Some(content) = search_env_candidates() {
        for line in content.lines() {
            let t = line.trim();
            if pk1_hex.is_none() && t.starts_with("POLYMARKET_KEY=") {
                let v = t["POLYMARKET_KEY=".len()..].trim().trim_matches('"').trim_matches('\'');
                pk1_hex = Some(v.to_string());
            } else if pk2_hex.is_none() && t.starts_with("PK2_KEY=") {
                let v = t["PK2_KEY=".len()..].trim().trim_matches('"').trim_matches('\'');
                pk2_hex = Some(v.to_string());
            }
        }
    }

    let pk1_str = pk1_hex.ok_or_else(|| "POLYMARKET_KEY not found in environment or .env".to_string())?;
    let pk1 = U256::from_be_hex(&pk1_str).ok_or_else(|| "Invalid POLYMARKET_KEY format".to_string())?;
    let pk1_addr = priv_to_addr_bytes(pk1).ok_or_else(|| "Failed deriving PK1 public address".to_string())?;

    let pk2 = if let Some(ref k2_str) = pk2_hex {
        if let Some(k2) = U256::from_be_hex(k2_str) {
            priv_to_addr_bytes(k2).map(|a2| (k2, a2))
        } else {
            None
        }
    } else {
        None
    };

    Ok(VaultSigners { pk1, pk1_addr, pk2 })
}

pub fn load_polymarket_config() -> PolymarketConfig {
    let mut proxy_wallet = std::env::var("PROXY_WALLET").unwrap_or_else(|_| "0x0000000000000000000000000000000000000000".to_string());
    let mut magic_signer = std::env::var("MAGIC_SIGNER").unwrap_or_else(|_| "0x0000000000000000000000000000000000000000".to_string());
    let mut api_key = std::env::var("POLY_API_KEY").unwrap_or_else(|_| "YOUR_POLY_API_KEY_UUID".to_string());
    let mut api_secret = std::env::var("POLY_SECRET").unwrap_or_else(|_| "YOUR_POLY_HMAC_SECRET_BASE64".to_string());
    let mut api_passphrase = std::env::var("POLY_PASSPHRASE").unwrap_or_else(|_| "YOUR_POLY_PASSPHRASE_HEX".to_string());
    let mut proxies: Vec<String> = Vec::new();

    if let Some(content) = search_env_candidates() {
        for line in content.lines() {
            let t = line.trim();
            if t.starts_with("PROXY_WALLET=") || t.starts_with("POLYMARKET_PROXY=") {
                if let Some(eq) = t.find('=') {
                    proxy_wallet = t[eq + 1..].trim().trim_matches('"').trim_matches('\'').to_string();
                }
            } else if t.starts_with("MAGIC_SIGNER=") {
                if let Some(eq) = t.find('=') {
                    magic_signer = t[eq + 1..].trim().trim_matches('"').trim_matches('\'').to_string();
                }
            } else if t.starts_with("POLY_API_KEY=") || t.starts_with("POLYMARKET_API_KEY=") {
                if let Some(eq) = t.find('=') {
                    api_key = t[eq + 1..].trim().trim_matches('"').trim_matches('\'').to_string();
                }
            } else if t.starts_with("POLY_SECRET=") || t.starts_with("POLYMARKET_API_SECRET=") {
                if let Some(eq) = t.find('=') {
                    api_secret = t[eq + 1..].trim().trim_matches('"').trim_matches('\'').to_string();
                }
            } else if t.starts_with("POLY_PASSPHRASE=") || t.starts_with("POLYMARKET_API_PASSPHRASE=") {
                if let Some(eq) = t.find('=') {
                    api_passphrase = t[eq + 1..].trim().trim_matches('"').trim_matches('\'').to_string();
                }
            } else if t.starts_with("PROXIES=") {
                if let Some(eq) = t.find('=') {
                    let raw = t[eq + 1..].trim().trim_matches('"').trim_matches('\'');
                    let list: Vec<String> = raw.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                    if !list.is_empty() {
                        proxies = list;
                    }
                }
            }
        }
    }

    PolymarketConfig {
        proxy_wallet,
        magic_signer,
        api_key,
        api_secret,
        api_passphrase,
        proxies,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keccak256_empty_and_transfer_selector() {
        let empty = keccak256(b"");
        assert_eq!(
            bytes_to_hex(&empty),
            "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
        let sel = keccak256(b"transfer(address,uint256)");
        assert_eq!(&bytes_to_hex(&sel[..4]), "0xa9059cbb");
    }

    #[test]
    fn test_ecdsa_sign_and_mathematical_verify() {
        let priv_k = U256::from_be_hex("0000000000000000000000000000000000000000000000000000000000001337").unwrap();
        let pub_q = Point::generator().scalar_mul(priv_k);
        let msg = keccak256(b"atomic balancer flashloan polygon 137");
        let sig = sign_hash_deterministic(priv_k, &msg).expect("sign ok");

        // Verify EIP-2 low-S invariant
        assert!(!sig.s.gt(U256::HALF_N), "Signature S must satisfy EIP-2 low-S");

        // Mathematical ECDSA verification: R' = (z * s^-1) * G + (r * s^-1) * Q
        let n = U256::N;
        let z = U256::from_be_bytes(&msg);
        let s_inv = sig.s.inv_mod_m(n);
        let u1 = z.mul_mod_m(s_inv, n);
        let u2 = sig.r.mul_mod_m(s_inv, n);
        let r_prime = Point::generator().scalar_mul(u1).add(pub_q.scalar_mul(u2));
        assert!(!r_prime.inf);
        assert_eq!(r_prime.x, sig.r);
        assert_eq!((r_prime.y.0[0] & 1) as u8, sig.y_parity);
    }

    #[test]
    fn test_create_address_prediction() {
        // Known Ethereum test vector: sender 0x6ac7ea33f8831ea9dcc53393aaa88b25a785dbf0, nonce 0 -> 0xcd234a471b72ba2f1ccf0a70fcaba648a5eecd8d
        let sender = parse_addr20("0x6ac7ea33f8831ea9dcc53393aaa88b25a785dbf0").unwrap();
        let c0 = predict_create_address(sender, 0);
        assert_eq!(bytes_to_hex(&c0), "0xcd234a471b72ba2f1ccf0a70fcaba648a5eecd8d");
        let c1 = predict_create_address(sender, 1);
        assert_eq!(bytes_to_hex(&c1), "0x343c43a37d37dff08ae8c4a11544c718abb4fcf8");
    }
}
