use crate::{
    Result,
    clock::{Clock, Version, join, reconcile},
    cluster::MAX_SIBLINGS,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Serialize, Deserialize)]
struct Record {
    clock: Clock,
    // None: reserva durável de relógio, sem guardar uma réplica da chave.
    key: Option<String>,
    versions: Vec<Version>,
}

pub struct Storage {
    _lock: File,
    log: File,
    clock: Clock,
    data: BTreeMap<String, Vec<Version>>,
    failed: bool,
}

impl Storage {
    pub fn open(directory: &Path, id: &str, mut members: Vec<String>) -> Result<Self> {
        fs::create_dir_all(directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("LOCK"))?;
        lock.try_lock()
            .map_err(|e| format!("diretório já está em uso: {e}"))?;
        members.sort();
        let identity = serde_json::to_vec(&(id, members))?;
        let identity_path = directory.join("identity.json");
        if identity_path.exists() {
            if fs::read(identity_path)? != identity {
                return Err("diretório pertence a outro nó ou membership".into());
            }
        } else {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(identity_path)?;
            file.write_all(&identity)?;
            file.sync_all()?;
        }
        let mut log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("wal.jsonl"))?;
        #[cfg(unix)]
        File::open(directory)?.sync_all()?;
        let mut bytes = Vec::new();
        log.read_to_end(&mut bytes)?;
        // Somente uma cauda sem '\n' pode ser uma escrita interrompida.
        let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |p| p + 1);
        if complete != bytes.len() {
            eprintln!(
                "WAL: removendo {} bytes da última escrita incompleta",
                bytes.len() - complete
            );
            log.set_len(complete as u64)?;
            log.sync_all()?;
        }
        let mut storage = Self {
            _lock: lock,
            log,
            clock: Clock::new(),
            data: BTreeMap::new(),
            failed: false,
        };
        for line in bytes[..complete]
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
        {
            let record: Record = serde_json::from_slice(line)?;
            storage.apply(record);
        }
        storage.log.seek(SeekFrom::End(0))?;
        Ok(storage)
    }

    fn apply(&mut self, record: Record) {
        self.clock = record.clock;
        if let Some(key) = record.key {
            self.data.insert(key, record.versions);
        }
    }

    fn commit(&mut self, record: Record) -> Result<()> {
        if self.failed {
            return Err("WAL indisponível; reinicie o nó após verificar o disco".into());
        }
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        // ACK só depois de persistir; uma falha de disco bloqueia novos ACKs.
        if let Err(error) = self
            .log
            .write_all(&bytes)
            .and_then(|()| self.log.sync_all())
        {
            self.failed = true;
            return Err(error.into());
        }
        self.apply(record);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Vec<Version>> {
        if self.failed {
            return Err("WAL indisponível".into());
        }
        Ok(self.data.get(key).cloned().unwrap_or_default())
    }

    pub fn observe(&mut self, context: &Clock) -> Result<()> {
        let mut clock = self.clock.clone();
        join(&mut clock, context);
        if clock != self.clock {
            self.commit(Record {
                clock,
                key: None,
                versions: vec![],
            })?;
        }
        Ok(())
    }

    pub fn issue(&mut self, id: &str, value: String, context: &Clock) -> Result<Version> {
        let mut clock = self.clock.clone();
        join(&mut clock, context);
        let count = clock.entry(id.into()).or_default();
        *count = count.checked_add(1).ok_or("relógio esgotado")?;
        self.commit(Record {
            clock: clock.clone(),
            key: None,
            versions: vec![],
        })?;
        Ok(Version { value, clock })
    }

    pub fn merge(&mut self, key: String, incoming: Vec<Version>) -> Result<()> {
        let mut clock = self.clock.clone();
        for version in &incoming {
            join(&mut clock, &version.clock);
        }
        let mut versions = self.get(&key)?;
        let old = versions.clone();
        versions.extend(incoming);
        let versions = reconcile(versions);
        if versions.len() > MAX_SIBLINGS {
            return Err("limite de 64 conflitos; resolva com contexto".into());
        }
        if old != versions || clock != self.clock {
            self.commit(Record {
                clock,
                key: Some(key),
                versions,
            })?;
        }
        Ok(())
    }
}
