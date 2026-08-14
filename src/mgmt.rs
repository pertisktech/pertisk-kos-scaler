use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use std::env;

const MAX_ADD_NODES: i32 = 16;

#[derive(Debug, Clone, Deserialize)]
pub struct Node {
    pub id: String,
    pub name: String,
    pub role: String,
    pub status: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub availability: String,
}

impl Node {
    pub fn is_ready(&self) -> bool {
        self.status == "ready"
    }
}

#[derive(Debug, Deserialize)]
struct Job {
    kind: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct LoginResponse {
    token: String,
}

#[derive(Clone)]
pub struct MgmtClient {
    client: Client,
    endpoint: String,
    cluster_id: String,
    token: Option<String>,
}

impl MgmtClient {
    pub fn new(endpoint: &str, cluster_id: &str) -> Result<Self> {
        let client = Client::builder()
            .build()
            .context("creating management HTTP client")?;
        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            cluster_id: cluster_id.to_owned(),
            token: env_nonempty("PERTISK_MGMT_TOKEN").or_else(|| env_nonempty("PERTISK_TOKEN")),
        })
    }

    pub async fn authenticate(&mut self) -> Result<()> {
        if self.token.is_some() {
            return Ok(());
        }
        let username = env_nonempty("PERTISK_MGMT_USERNAME").ok_or_else(|| {
            anyhow!("set PERTISK_MGMT_TOKEN or PERTISK_MGMT_USERNAME/PERTISK_MGMT_PASSWORD")
        })?;
        let password = env_nonempty("PERTISK_MGMT_PASSWORD").ok_or_else(|| {
            anyhow!("PERTISK_MGMT_PASSWORD is required when using username login")
        })?;
        let response = self
            .client
            .post(format!("{}/api/auth/login", self.endpoint))
            .json(&serde_json::json!({ "username": username, "password": password }))
            .send()
            .await
            .context("logging in to pertisk-mgmt")?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("pertisk-mgmt login failed: {status}: {body}");
        }
        let login: LoginResponse = response
            .json()
            .await
            .context("parsing pertisk-mgmt login response")?;
        self.token = Some(login.token);
        Ok(())
    }

    pub async fn workers(&self) -> Result<Vec<Node>> {
        let response = self.send(self.client.get(self.nodes_url())).await?;
        let nodes: Vec<Node> = response
            .json()
            .await
            .context("parsing management node list")?;
        Ok(nodes
            .into_iter()
            .filter(|node| node.role == "worker")
            .collect())
    }

    pub async fn add_workers(
        &self,
        count: i32,
        memory: Option<i64>,
        cores: Option<i64>,
        disk_gb: Option<i64>,
    ) -> Result<()> {
        #[derive(Serialize)]
        struct AddWorkers {
            role: &'static str,
            count: i32,
            memory: Option<i64>,
            cores: Option<i64>,
            disk_gb: Option<i64>,
        }
        let mut remaining = count;
        while remaining > 0 {
            let batch = remaining.min(MAX_ADD_NODES);
            self.send(self.client.post(self.nodes_url()).json(&AddWorkers {
                role: "worker",
                count: batch,
                memory,
                cores,
                disk_gb,
            }))
            .await?;
            remaining -= batch;
        }
        Ok(())
    }

    pub async fn remove_worker(&self, node_id: &str) -> Result<()> {
        self.send(
            self.client
                .delete(format!("{}/{}", self.nodes_url(), node_id)),
        )
        .await?;
        Ok(())
    }

    pub async fn has_active_lifecycle_job(&self) -> Result<bool> {
        let url = format!("{}/api/clusters/{}/jobs", self.endpoint, self.cluster_id);
        let jobs: Vec<Job> = self
            .send(self.client.get(url))
            .await?
            .json()
            .await
            .context("parsing management job list")?;
        Ok(jobs.into_iter().any(is_active_lifecycle_job))
    }

    fn nodes_url(&self) -> String {
        format!("{}/api/clusters/{}/nodes", self.endpoint, self.cluster_id)
    }

    fn authenticated(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    async fn send(&self, request: RequestBuilder) -> Result<reqwest::Response> {
        let response = self
            .authenticated(request)
            .send()
            .await
            .context("calling pertisk-mgmt")?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!("pertisk-mgmt request failed: {status}: {body}"))
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn is_active_lifecycle_job(job: Job) -> bool {
    matches!(
        job.kind.as_str(),
        "add_node" | "remove_node" | "upgrade_cluster"
    ) && matches!(job.status.as_str(), "queued" | "running")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_add_node_is_active() {
        assert!(is_active_lifecycle_job(Job {
            kind: "add_node".into(),
            status: "queued".into(),
        }));
    }

    #[test]
    fn succeeded_upgrade_is_not_active() {
        assert!(!is_active_lifecycle_job(Job {
            kind: "upgrade_cluster".into(),
            status: "succeeded".into(),
        }));
    }

    #[test]
    fn unrelated_job_is_not_active() {
        assert!(!is_active_lifecycle_job(Job {
            kind: "reboot_node".into(),
            status: "running".into(),
        }));
    }
}
