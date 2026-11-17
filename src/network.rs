use crate::{
    Result,
    clock::{Clock, Version},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Ping,
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
        #[serde(default)]
        context: Clock,
    },
    ReplicaGet {
        key: String,
    },
    ReplicaPut {
        key: String,
        versions: Vec<Version>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerStatus {
    pub node: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Pong {
        node: String,
    },
    Read {
        versions: Vec<Version>,
        context: Clock,
        replicas: Vec<PeerStatus>,
        repair: Vec<PeerStatus>,
    },
    Written {
        version: Version,
        replicas: Vec<PeerStatus>,
    },
    Replica {
        versions: Vec<Version>,
    },
    Ack,
    Error {
        message: String,
    },
}

pub const MAX_FRAME: usize = 4 * 1024 * 1024;

fn remaining(deadline: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| "prazo da comunicação esgotado".into())
}

pub fn send<T: Serialize>(stream: &mut TcpStream, value: &T, deadline: Instant) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_FRAME {
        return Err("mensagem excede 4 MiB".into());
    }
    bytes.push(b'\n');
    let mut sent = 0;
    while sent < bytes.len() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        let count = stream.write(&bytes[sent..])?;
        if count == 0 {
            return Err("conexão fechada durante envio".into());
        }
        sent += count;
    }
    Ok(())
}

pub fn receive<T: DeserializeOwned>(stream: &mut TcpStream, deadline: Instant) -> Result<T> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err("conexão terminou antes da quebra de linha".into());
        }
        let end = buffer[..count].iter().position(|b| *b == b'\n');
        bytes.extend_from_slice(&buffer[..end.unwrap_or(count)]);
        if bytes.len() > MAX_FRAME {
            return Err("mensagem excede 4 MiB".into());
        }
        if end.is_some() {
            return Ok(serde_json::from_slice(&bytes)?);
        }
    }
}

// Uma conexão por RPC; o prazo absoluto cobre conectar, enviar e receber.
pub fn rpc(addr: SocketAddr, request: &Request, timeout: Duration) -> Result<Response> {
    let deadline = Instant::now() + timeout;
    let mut stream = TcpStream::connect_timeout(&addr, remaining(deadline)?)?;
    stream.set_nodelay(true)?;
    send(&mut stream, request, deadline)?;
    receive(&mut stream, deadline)
}
