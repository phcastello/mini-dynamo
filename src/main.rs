mod clock;
mod cluster;
mod network;
mod node;
mod storage;

use clock::Clock;
use cluster::Config;
use network::{Request, Response};
use node::Node;
use std::{
    env, fs,
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::Arc,
    time::Duration,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const USAGE: &str = "mini-dynamo node <cluster.json> <id> <diretório>\nmini-dynamo ping <IP:porta>\nmini-dynamo get <IP:porta> <chave>\nmini-dynamo put <IP:porta> <chave> <valor> [contexto.json]\nmini-dynamo ring <cluster.json> <chave>\nmini-dynamo check <cluster.json>\n\nGET imprime versões e contexto. Salve a resposta GET em arquivo para usá-la no PUT.";

fn client(addr: &str, request: Request) -> Result<()> {
    let response = network::rpc(addr.parse()?, &request, Duration::from_secs(90))?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    if let Response::Error { message } = response {
        return Err(message.into());
    }
    Ok(())
}

fn run() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["node", file, id, directory] => {
            let config = Config::load(file)?;
            let addr = config.peer(id)?.addr.ok_or("nó sem endereço")?;
            let node = Arc::new(Node::open(id.to_string(), config, Path::new(directory))?);
            // IP anunciado pode ser da VPN. Escutar interfaces locais na mesma família.
            let bind = SocketAddr::new(
                if addr.is_ipv4() {
                    "0.0.0.0".parse()?
                } else {
                    "::".parse()?
                },
                addr.port(),
            );
            let listener = TcpListener::bind(bind)?;
            eprintln!("nó {id}; escutando {bind}; anunciado {addr}; dados {directory}");
            node.serve(listener)
        }
        ["ping", addr] => client(addr, Request::Ping),
        ["get", addr, key] => client(
            addr,
            Request::Get {
                key: key.to_string(),
            },
        ),
        ["put", addr, key, value] => client(
            addr,
            Request::Put {
                key: key.to_string(),
                value: value.to_string(),
                context: Clock::new(),
            },
        ),
        ["put", addr, key, value, file] => {
            let json: serde_json::Value = serde_json::from_slice(&fs::read(file)?)?;
            let context = serde_json::from_value(json.get("context").cloned().unwrap_or(json))?;
            client(
                addr,
                Request::Put {
                    key: key.to_string(),
                    value: value.to_string(),
                    context,
                },
            )
        }
        ["ring", file, key] => {
            let config = Config::load(file)?;
            let replicas: Vec<_> = cluster::replicas(&config, key).into_iter().collect();
            println!("{}", serde_json::to_string_pretty(&replicas)?);
            Ok(())
        }
        ["check", file] => {
            let config = Config::load(file)?;
            let mut failures = 0;
            for peer in &config.nodes {
                let result = peer
                    .addr
                    .ok_or("sem endereço".into())
                    .and_then(|addr| network::rpc(addr, &Request::Ping, config.timeout()));
                match result {
                    Ok(Response::Pong { node }) if node == peer.id => {
                        println!("{} OK {:?}", peer.id, peer.addr)
                    }
                    other => {
                        failures += 1;
                        println!("{} INDISPONÍVEL {other:?}", peer.id);
                    }
                }
            }
            if failures > 0 {
                return Err(format!("{failures} nó(s) indisponível(is)").into());
            }
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
