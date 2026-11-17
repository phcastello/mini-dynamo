use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type Clock = BTreeMap<String, u64>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub value: String,
    pub clock: Clock,
}

pub fn join(into: &mut Clock, other: &Clock) {
    for (node, count) in other {
        let current = into.entry(node.clone()).or_default();
        *current = (*current).max(*count);
    }
}

// a < b: nenhum componente de a é maior e pelo menos um é menor.
pub fn before(a: &Clock, b: &Clock) -> bool {
    a != b
        && a.iter()
            .all(|(node, count)| *count <= *b.get(node).unwrap_or(&0))
}

pub fn context(versions: &[Version]) -> Clock {
    let mut clock = Clock::new();
    for version in versions {
        join(&mut clock, &version.clock);
    }
    clock
}

// União comutativa, associativa e idempotente das versões não dominadas.
pub fn reconcile(mut versions: Vec<Version>) -> Vec<Version> {
    versions.sort_by(|a, b| a.clock.cmp(&b.clock).then(a.value.cmp(&b.value)));
    versions.dedup();
    versions
        .iter()
        .filter(|a| !versions.iter().any(|b| before(&a.clock, &b.clock)))
        .cloned()
        .collect()
}
