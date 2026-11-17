use crate::{
    Result,
    clock::{context, reconcile},
    cluster::{self, Config, R, W, validate_key, validate_value},
    network::{self, PeerStatus, Request, Response},
    storage::Storage,
};
use std::{
    net::TcpListener,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Instant,
};

pub struct Node {
    pub id: String,
    pub config: Config,
    storage: Mutex<Storage>,
}

impl Node {
    pub fn open(id: String, config: Config, directory: &Path) -> Result<Self> {
        config.validate()?;
        config
            .peer(&id)?
            .addr
            .ok_or("nó sem endereço configurado")?;
        let members = config.nodes.iter().map(|n| n.id.clone()).collect();
        let storage = Mutex::new(Storage::open(directory, &id, members)?);
        Ok(Self {
            id,
            config,
            storage,
        })
    }

    pub fn handle(&self, request: Request) -> Response {
        self.try_handle(request)
            .unwrap_or_else(|e| Response::Error {
                message: e.to_string(),
            })
    }

    fn try_handle(&self, request: Request) -> Result<Response> {
        match request {
            Request::Ping => Ok(Response::Pong {
                node: self.id.clone(),
            }),
            Request::Get { key } => {
                validate_key(&key)?;
                let replies = self.fanout(&key, Request::ReplicaGet { key: key.clone() });
                let mut versions = Vec::new();
                for (_, reply) in &replies {
                    if let Response::Replica { versions: found } = reply {
                        versions.extend(found.clone());
                    }
                }
                let replicas = statuses(&replies, false);
                require_quorum(&replicas, R, "leitura")?;
                let versions = reconcile(versions);
                let context = context(&versions);
                self.storage
                    .lock()
                    .map_err(|_| "mutex de storage inválido")?
                    .observe(&context)?;
                // Repair faz merge, nunca substitui: uma escrita concorrente é preservada.
                let repair = if versions.is_empty() {
                    vec![]
                } else {
                    statuses(
                        &self.fanout(
                            &key,
                            Request::ReplicaPut {
                                key: key.clone(),
                                versions: versions.clone(),
                            },
                        ),
                        true,
                    )
                };
                Ok(Response::Read {
                    versions,
                    context,
                    replicas,
                    repair,
                })
            }
            Request::Put {
                key,
                value,
                context,
            } => {
                validate_key(&key)?;
                validate_value(&value)?;
                self.config.validate_clock(&context)?;
                let version = self
                    .storage
                    .lock()
                    .map_err(|_| "mutex de storage inválido")?
                    .issue(&self.id, value, &context)?;
                // Não segurar o mutex durante rede: evita deadlock de coordenadores.
                let replies = self.fanout(
                    &key,
                    Request::ReplicaPut {
                        key: key.clone(),
                        versions: vec![version.clone()],
                    },
                );
                let replicas = statuses(&replies, true);
                require_quorum(
                    &replicas,
                    W,
                    "escrita (resultado pode ser parcial; não há rollback)",
                )?;
                Ok(Response::Written { version, replicas })
            }
            Request::ReplicaGet { key } => {
                self.check_replica(&key)?;
                let versions = self
                    .storage
                    .lock()
                    .map_err(|_| "mutex de storage inválido")?
                    .get(&key)?;
                Ok(Response::Replica { versions })
            }
            Request::ReplicaPut { key, versions } => {
                self.check_replica(&key)?;
                self.config.validate_versions(&versions)?;
                self.storage
                    .lock()
                    .map_err(|_| "mutex de storage inválido")?
                    .merge(key, versions)?;
                Ok(Response::Ack)
            }
        }
    }

    fn check_replica(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        if !cluster::replicas(&self.config, key)
            .iter()
            .any(|n| n.id == self.id)
        {
            return Err("este nó não é réplica desta chave".into());
        }
        Ok(())
    }

    fn fanout(&self, key: &str, request: Request) -> Vec<(String, Response)> {
        thread::scope(|scope| {
            let workers: Vec<_> = cluster::replicas(&self.config, key)
                .into_iter()
                .map(|peer| {
                    let request = &request;
                    scope.spawn(move || {
                        let response = if peer.id == self.id {
                            self.handle(request.clone())
                        } else {
                            peer.addr
                                .ok_or_else(|| "endereço ainda não configurado".into())
                                .and_then(|addr| network::rpc(addr, request, self.config.timeout()))
                                .unwrap_or_else(|e: Box<dyn std::error::Error + Send + Sync>| {
                                    Response::Error {
                                        message: e.to_string(),
                                    }
                                })
                        };
                        (peer.id.clone(), response)
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| {
                    worker.join().unwrap_or_else(|_| {
                        (
                            "worker".into(),
                            Response::Error {
                                message: "falha interna do worker".into(),
                            },
                        )
                    })
                })
                .collect()
        })
    }

    pub fn serve(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        let active = Arc::new(AtomicUsize::new(0));
        for connection in listener.incoming() {
            let mut stream = connection?;
            if active.fetch_add(1, Ordering::Relaxed) >= 128 {
                active.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
            let node = self.clone();
            let active = active.clone();
            thread::spawn(move || {
                let deadline = Instant::now() + node.config.timeout() * 8;
                let response = match network::receive(&mut stream, deadline) {
                    Ok(request) => node.handle(request),
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                };
                if let Err(e) = network::send(&mut stream, &response, deadline) {
                    eprintln!("resposta: {e}");
                }
                active.fetch_sub(1, Ordering::Relaxed);
            });
        }
        Ok(())
    }
}

fn statuses(replies: &[(String, Response)], write: bool) -> Vec<PeerStatus> {
    replies
        .iter()
        .map(|(node, reply)| {
            let ok = matches!(
                (write, reply),
                (true, Response::Ack) | (false, Response::Replica { .. })
            );
            let error = if ok {
                None
            } else {
                Some(match reply {
                    Response::Error { message } => message.clone(),
                    _ => "resposta inesperada".into(),
                })
            };
            PeerStatus {
                node: node.clone(),
                ok,
                error,
            }
        })
        .collect()
}

fn require_quorum(replies: &[PeerStatus], required: usize, operation: &str) -> Result<()> {
    let count = replies.iter().filter(|r| r.ok).count();
    if count < required {
        return Err(format!(
            "quórum de {operation} insuficiente: {count}/{required}; {}",
            serde_json::to_string(replies)?
        )
        .into());
    }
    Ok(())
}
