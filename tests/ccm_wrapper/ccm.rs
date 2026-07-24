//! Simple CLI ccm wrapper intended for testing network traffic.
//!
//! To create and start a cluster, build a topology (see [`TopologySpecBuilder`] for how to construct a topology), then:
//! ```no_run
//! let mut cluster = Ccm::create_cluster(name, &topology, ip_prefix, 8000, scylla_version).unwrap();
//! Ccm::start_cluster(&mut cluster).unwrap();
//! ```
//! This will create a cluster of given topology with alternator running on every node on the same address as a node and given port.
//!

use crate::ccm_wrapper::cluster::*;
use crate::ccm_wrapper::topology_spec::*;
use anyhow::Context;
use itertools::Itertools;
use std::ffi::OsStr;
use std::ops::{Deref, DerefMut};
use std::process::{Command, Output, Stdio};

pub(crate) struct Ccm;

impl Ccm {
    // Logic behind cluster creation comes from the way ccm operates. Initially it creates given amount of datacenters,
    // each with 1 rack and given amount of nodes on it. For example ccm create -n 2:1:2 would result in the following structure:
    // dc1
    //     RAC1
    //         node1
    //         node2
    // dc2
    //     RAC1
    //         node3
    // dc3
    //     RAC1
    //         node4
    //         node5
    // Nodes are given names and ips ordered by datacenters. Ips are of the form prefix{n} where n is the index of the node.
    // Adding node on a different rack than the first one, has to be done with ccm add with --data-center and --rack.
    // That means, that when we want to create cluster with, for example 3 datacenters, each with 2 nodes on the first rack, 1 on second, and 1 on third,
    // we first have to run ccm create -n 2:2:2 and then 6 times ccm add. Since first nodes have imposed names and ips, in order to be consistent
    // we give the nodes that are being added, the first free ip with given prefix and matching name. This means that we end up with:
    // dc1
    //     RAC1
    //         node1
    //         node2
    //     RAC2
    //         node7
    //     RAC3
    //         node8
    // dc2
    //     RAC1
    //         node3
    //         node4
    //     RAC2
    //         node9
    //     RAC3
    //         node10
    // dc3
    //     RAC1
    //         node5
    //         node6
    //     RAC2
    //         node11
    //     RAC3
    //         node12
    pub(crate) fn create_cluster(
        cluster_name: String,
        topology: &TopologySpec,
        ip_prefix: IpPrefix,
        alternator_port: u16,
        scylla_version: String,
    ) -> anyhow::Result<Cluster> {
        Self::create_cluster_with_node_config(
            cluster_name,
            topology,
            ip_prefix,
            alternator_port,
            scylla_version,
            &[],
        )
    }

    pub(crate) fn create_cluster_with_node_config(
        cluster_name: String,
        topology: &TopologySpec,
        ip_prefix: IpPrefix,
        alternator_port: u16,
        scylla_version: String,
        node_config: &[String],
    ) -> anyhow::Result<Cluster> {
        // String of form nodes_in_dc1_RAC1:nodes_in_dc2_RAC1:nodes_in_dc3_RAC1...
        let first_rack_nodes_per_dc = topology.datacenters.iter().map(|dc| dc.racks[0]).join(":");

        CcmCommandRunner::create_cluster(
            &cluster_name,
            &first_rack_nodes_per_dc,
            ip_prefix.as_str(),
            &scylla_version,
        )?;

        match Self::create_cluster_impl(
            cluster_name.clone(),
            ip_prefix,
            topology,
            scylla_version,
            alternator_port,
            node_config,
        ) {
            Ok(cluster) => Ok(cluster),
            Err(e) => {
                let err = e.context("Cluster creation failed.");
                // Cluster is already existing so it needs to be removed.
                match CcmCommandRunner::remove_cluster(&cluster_name) {
                    Ok(_) => Err(err),
                    Err(remove_error) => Err(err.context(format!(
                        "Failed to clean up after error during creation: {remove_error}"
                    ))),
                }
            }
        }
    }

    fn create_cluster_impl(
        cluster_name: String,
        ip_prefix: IpPrefix,
        topology: &TopologySpec,
        scylla_version: String,
        alternator_port: u16,
        node_config: &[String],
    ) -> anyhow::Result<Cluster> {
        let mut cluster = Cluster::new(cluster_name, ip_prefix, scylla_version);

        let total_first_rack_nodes: usize = topology.datacenters.iter().map(|dc| dc.racks[0]).sum();

        // These 2 counters determine the index (and thus name/IP) of each node.
        // Nodes in RAC1 of each datacenter are created by ccm create and receive indices 1..=total_first_rack_nodes,
        // so we track them separately with first_rack_nodes_count. This is necessary, because we still have
        // to attach alternator to them with ccm {node_name} updateconf.
        // Nodes on additional racks are added later with ccm add and receive
        // indices starting from total_first_rack_nodes+1, tracked by total_nodes_count.
        let mut first_rack_nodes_count: usize = 0;
        let mut total_nodes_count: usize = total_first_rack_nodes;

        // We can't run add node asynchronously because of race condition to cluster.conf file, but we can do it with updateconf.
        // Once the node is added, alternator adding process is spawned. Here we keep all these processes and wait on them later.
        let mut pending_updateconfs = BatchCcmHandler::new("updateconf");

        // Run the main cluster-building logic in a closure so that we can
        // always run wait_all() afterwards, even if an error occurs.
        let main_result: anyhow::Result<()> = (|| {
            for (datacenter_idx, datacenter_spec) in topology.datacenters.iter().enumerate() {
                let dc_name = format!("dc{}", datacenter_idx + 1);
                let mut datacenter = Datacenter::new(dc_name.clone());
                for (rack_idx, nodes_num) in datacenter_spec.racks.iter().enumerate() {
                    let rack_name = format!("RAC{}", rack_idx + 1);
                    let mut rack = Rack::new(rack_name.clone());
                    for _ in 0..*nodes_num {
                        let node_idx = match rack_idx {
                            0 => {
                                first_rack_nodes_count += 1;
                                first_rack_nodes_count
                            }
                            _ => {
                                total_nodes_count += 1;
                                total_nodes_count
                            }
                        };
                        let ip = format!("{}{}", cluster.ip_prefix.as_str(), node_idx);
                        let node_name = format!("node{}", node_idx);
                        // If node is not on the first rack, we create it.
                        if rack_idx > 0 {
                            CcmCommandRunner::add_node(&node_name, &ip, &dc_name, &rack_name)?;
                        }
                        let node = Node::new(node_name, ip, alternator_port);
                        let child = Self::add_alternator_to_node(&node, node_config)?;
                        pending_updateconfs.push(node.name.clone(), child);
                        rack.add_node(node);
                    }
                    datacenter.add_rack(rack);
                }
                cluster.add_datacenter(datacenter);
            }
            Ok(())
        })();
        let wait_result = pending_updateconfs.wait_all();
        match (main_result, wait_result) {
            (Ok(()), Ok(())) => Ok(cluster),
            (Err(e), Ok(())) => Err(e),
            (Ok(()), Err(e)) => Err(e),
            (Err(e1), Err(e2)) => Err(e1.context(format!(
                "Additionally, waiting for pending 'ccm updateconf' processes failed: {e2}"
            ))),
        }
    }

    pub(crate) fn remove_cluster(cluster: &mut Cluster) -> anyhow::Result<()> {
        CcmCommandRunner::remove_cluster(&cluster.name)
    }

    pub(crate) fn start_cluster(cluster: &mut Cluster) -> anyhow::Result<()> {
        CcmCommandRunner::start_cluster()?;

        cluster.is_up = true;
        for node in cluster.nodes_mut() {
            node.is_up = true;
        }
        Ok(())
    }

    pub(crate) fn stop_cluster(cluster: &mut Cluster) -> anyhow::Result<()> {
        CcmCommandRunner::stop_cluster()?;
        cluster.is_up = false;
        for node in cluster.nodes_mut() {
            node.is_up = false;
        }
        Ok(())
    }

    pub(crate) fn stop_node(node: &mut Node) -> anyhow::Result<()> {
        CcmCommandRunner::stop_node(&node.name)?;
        node.is_up = false;
        Ok(())
    }

    pub(crate) fn start_node(node: &mut Node) -> anyhow::Result<()> {
        CcmCommandRunner::start_node(&node.name)?;
        node.is_up = true;
        Ok(())
    }

    fn add_alternator_to_node(
        node: &Node,
        node_config: &[String],
    ) -> anyhow::Result<std::process::Child> {
        let address = format!("alternator_address:{}", node.ip);
        let port = format!("alternator_port:{}", node.alternator_port);
        let mut config = vec![
            address,
            port,
            "alternator_write_isolation:always".to_string(),
        ];
        config.extend_from_slice(node_config);
        let config_refs: Vec<&str> = config.iter().map(String::as_str).collect();
        CcmCommandRunner::spawn_update_node_conf(&node.name, &config_refs)
    }
}

// Struct used to communicate with cli.
pub(crate) struct CcmCommandRunner;

impl CcmCommandRunner {
    fn run<I, S>(args: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args: Vec<std::ffi::OsString> = args
            .into_iter()
            .map(|a| a.as_ref().to_os_string())
            .collect();

        let command_str = format!("ccm {}", args.iter().map(|a| a.to_string_lossy()).join(" "));
        let output: Output = Command::new("ccm")
            .args(args)
            .output()
            .with_context(|| format!("Failed to run {}", command_str))?;

        // ccm outputs errors on stdout, but tracebacks/underlying tool
        // output (e.g. Docker, download errors) frequently land on stderr;
        // include both so failures are diagnosable.
        anyhow::ensure!(
            output.status.success(),
            "\nCommand failed: {}\nccm stdout: {}\nccm stderr: {}",
            command_str,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        Ok(())
    }

    fn create_cluster(
        cluster_name: &str,
        first_rack_nodes_per_dc: &str,
        ip_prefix: &str,
        scylla_version: &str,
    ) -> anyhow::Result<()> {
        Self::run([
            "create",
            cluster_name,
            "-n",
            first_rack_nodes_per_dc,
            "-i",
            ip_prefix,
            "--scylla",
            "-v",
            scylla_version,
        ])
    }

    fn add_node(node_name: &str, ip: &str, dc_name: &str, rack_name: &str) -> anyhow::Result<()> {
        Self::run([
            "add",
            node_name,
            "-i",
            ip,
            "--data-center",
            dc_name,
            "--rack",
            rack_name,
            "--scylla",
        ])
    }

    // The start/stop commands do not take any arguments, they just run on currently active cluster.
    // Newly created cluster automatically becomes the active one and it remains after stopping
    // which allows us to reset the clusters during tests.
    fn start_cluster() -> anyhow::Result<()> {
        Self::run(["start", "--wait-for-binary-proto", "--wait-other-notice"])
    }

    fn stop_cluster() -> anyhow::Result<()> {
        Self::run(["stop"])
    }

    fn stop_node(node_name: &str) -> anyhow::Result<()> {
        Self::run([node_name, "stop"])
    }

    fn start_node(node_name: &str) -> anyhow::Result<()> {
        Self::run([
            node_name,
            "start",
            "--wait-for-binary-proto",
            "--wait-other-notice",
        ])
    }

    fn remove_cluster(cluster_name: &str) -> anyhow::Result<()> {
        Self::run(["remove", cluster_name])
    }

    // This command is run by different processes to speed up cluster creation.
    // It can be done safely because each process only changes 1 separate config file.
    fn spawn_update_node_conf(
        node_name: &str,
        conf: &[&str],
    ) -> anyhow::Result<std::process::Child> {
        let mut args: Vec<&str> = vec![node_name, "updateconf"];
        args.extend_from_slice(conf);

        let command_str = format!("ccm {}", args.join(" "));
        Command::new("ccm")
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("Failed to spawn: {}", command_str))
    }
}

// This struct is used to handle child processes spawned by ccm.
pub(crate) struct BatchCcmHandler {
    command: String,
    // Node name and process.
    pending: Vec<(String, std::process::Child)>,
}

impl BatchCcmHandler {
    fn new(command: &str) -> Self {
        Self {
            command: command.to_string(),
            pending: Vec::new(),
        }
    }

    fn push(&mut self, node_name: String, child: std::process::Child) {
        self.pending.push((node_name, child));
    }

    fn wait_all(self) -> anyhow::Result<()> {
        let mut errors: Vec<String> = Vec::new();
        for (node_name, child) in self.pending {
            match child.wait_with_output() {
                Ok(output) if output.status.success() => {}
                Ok(output) => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    errors.push(format!(
                        "{} for {} failed: {}",
                        self.command, node_name, stdout
                    ))
                }
                Err(e) => errors.push(format!(
                    "{} for {} wait failed: {}",
                    self.command, node_name, e
                )),
            }
        }
        anyhow::ensure!(
            errors.is_empty(),
            "Some {} commands failed:\n{}",
            self.command,
            errors.join("\n")
        );
        Ok(())
    }
}

// Guard for Cluster that removes the cluster on drop.
pub(crate) struct ClusterGuard(pub(crate) Cluster);

impl Deref for ClusterGuard {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ClusterGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Drop for ClusterGuard {
    fn drop(&mut self) {
        let _ = Ccm::remove_cluster(&mut self.0);
    }
}
