use crate::{
    Result,
    clock::{Clock, Version},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fs, net::SocketAddr, path::Path, time::Duration};

pub const N: usize = 3;
pub const R: usize = 2;
pub const W: usize = 2;
pub const MAX_SIBLINGS: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub id: String,
    // null reserva a identidade de um nó ainda sem endereço conhecido.
    pub addr: Option<SocketAddr>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub timeout_ms: u64,
    pub nodes: Vec<Peer>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let config: Self = serde_json::from_slice(&fs::read(path)?)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.nodes.len() != 4 || !(50..=10_000).contains(&self.timeout_ms) {
            return Err("configure exatamente 4 nós e timeout_ms entre 50 e 10000".into());
        }
        let mut ids = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        for node in &self.nodes {
            if node.id.is_empty() || node.id.len() > 40 || !ids.insert(&node.id) {
                return Err("IDs devem ser únicos e ter de 1 a 40 bytes".into());
            }
            if let Some(addr) = node.addr
                && (addr.port() == 0 || addr.ip().is_unspecified() || !addresses.insert(addr))
            {
                return Err("endereços devem ser únicos, com IP explícito e porta não zero".into());
            }
        }
        Ok(())
    }

    pub fn peer(&self, id: &str) -> Result<&Peer> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .ok_or_else(|| format!("nó desconhecido: {id}").into())
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    pub fn validate_clock(&self, clock: &Clock) -> Result<()> {
        for (node, count) in clock {
            self.peer(node)?;
            if *count == 0 {
                return Err("vetor deve omitir componentes zero".into());
            }
        }
        Ok(())
    }

    pub fn validate_versions(&self, versions: &[Version]) -> Result<()> {
        if versions.is_empty() || versions.len() > MAX_SIBLINGS {
            return Err("replicação exige de 1 a 64 versões".into());
        }
        for version in versions {
            validate_value(&version.value)?;
            self.validate_clock(&version.clock)?;
            if version.clock.is_empty() {
                return Err("versão sem relógio".into());
            }
        }
        Ok(())
    }
}

pub fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 256 {
        return Err("chave deve ter de 1 a 256 bytes UTF-8".into());
    }
    Ok(())
}

pub fn validate_value(value: &str) -> Result<()> {
    if value.len() > 16_384 {
        return Err("valor limitado a 16 KiB UTF-8".into());
    }
    Ok(())
}

// FNV-1a de 64 bits: não depende de sementes aleatórias nem da plataforma.
pub fn hash(text: &str) -> u64 {
    text.bytes().fold(0xcbf29ce484222325, |h, byte| {
        (h ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

pub fn replicas<'a>(config: &'a Config, key: &str) -> Vec<&'a Peer> {
    let mut ring: Vec<_> = config.nodes.iter().collect();
    ring.sort_by(|a, b| hash(&a.id).cmp(&hash(&b.id)).then(a.id.cmp(&b.id)));
    let start = ring
        .iter()
        .position(|n| hash(&n.id) >= hash(key))
        .unwrap_or(0);
    (0..N)
        .map(|offset| ring[(start + offset) % ring.len()])
        .collect()
}
