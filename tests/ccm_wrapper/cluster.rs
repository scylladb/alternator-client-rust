use anyhow::Context;

// Struct for ip prefix of form X.X.X.
#[derive(Debug, Clone)]
pub(crate) struct IpPrefix(String);

impl IpPrefix {
    pub(crate) fn new(ip_prefix: &str) -> anyhow::Result<Self> {
        Self::validate_ip_prefix(ip_prefix)?;
        Ok(Self(ip_prefix.to_string()))
    }
    fn validate_ip_prefix(ip_prefix: &str) -> anyhow::Result<()> {
        let prefix = ip_prefix
            .strip_suffix('.')
            .context("IP prefix must end with '.'")?;

        let octets: Vec<&str> = prefix.split('.').collect();
        anyhow::ensure!(
            octets.len() == 3,
            "IP prefix must have exactly 3 octets, got {}",
            octets.len()
        );

        for octet in octets {
            octet
                .parse::<u8>()
                .with_context(|| format!("Invalid octet '{}' in IP prefix", octet))?;
        }

        Ok(())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub(crate) name: String,
    pub(crate) ip: String,
    pub(crate) alternator_port: u16,
    pub(crate) is_up: bool,
}

impl Node {
    pub(crate) fn new(name: String, ip: String, alternator_port: u16) -> Self {
        Self {
            name,
            ip,
            alternator_port,
            is_up: false,
        }
    }

    pub(crate) fn address(&self) -> String {
        format!("http://{}:{}", self.ip, self.alternator_port)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Rack {
    pub(crate) name: String,
    nodes: Vec<Node>,
}

impl Rack {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            nodes: Vec::new(),
        }
    }

    pub(crate) fn node_ips(&self) -> Vec<&str> {
        self.nodes.iter().map(|node| node.ip.as_str()).collect()
    }

    pub(crate) fn working_nodes_ips(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|node| node.is_up)
            .map(|node| node.ip.as_str())
            .collect()
    }

    pub(crate) fn add_node(&mut self, node: Node) {
        self.nodes.push(node);
    }

    pub(crate) fn first_working_node_address(&self) -> Option<String> {
        self.nodes
            .iter()
            .find(|node| node.is_up)
            .map(|node| node.address())
    }

    pub(crate) fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub(crate) fn nodes_mut(&mut self) -> &mut [Node] {
        &mut self.nodes
    }

    pub(crate) fn node_mut(&mut self, node_idx: usize) -> Option<&mut Node> {
        self.nodes_mut().get_mut(node_idx)
    }
}

pub(crate) struct Datacenter {
    pub(crate) name: String,
    racks: Vec<Rack>,
}

impl Datacenter {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            racks: Vec::new(),
        }
    }

    pub(crate) fn add_rack(&mut self, rack: Rack) {
        self.racks.push(rack);
    }

    pub(crate) fn node_ips(&self) -> Vec<&str> {
        self.racks
            .iter()
            .flat_map(|rack| rack.nodes.iter())
            .map(|node| node.ip.as_str())
            .collect()
    }

    pub(crate) fn working_nodes_ips(&self) -> Vec<&str> {
        self.racks
            .iter()
            .flat_map(|rack| rack.nodes.iter())
            .filter(|node| node.is_up)
            .map(|node| node.ip.as_str())
            .collect()
    }

    pub(crate) fn first_working_node_address(&self) -> Option<String> {
        self.racks
            .iter()
            .flat_map(|rack| rack.nodes.iter())
            .find(|node| node.is_up)
            .map(|node| node.address())
    }

    pub(crate) fn racks(&self) -> &[Rack] {
        &self.racks
    }

    pub(crate) fn racks_mut(&mut self) -> &mut [Rack] {
        &mut self.racks
    }

    pub(crate) fn node_mut(&mut self, rack_idx: usize, node_idx: usize) -> Option<&mut Node> {
        self.racks.get_mut(rack_idx)?.node_mut(node_idx)
    }
}

pub(crate) struct Cluster {
    pub(crate) name: String,
    pub(crate) ip_prefix: IpPrefix,
    pub(crate) datacenters: Vec<Datacenter>,
    pub(crate) scylla_version: String,
    pub(crate) is_up: bool,
}

impl Cluster {
    pub(crate) fn new(name: String, ip_prefix: IpPrefix, scylla_version: String) -> Self {
        Self {
            name,
            ip_prefix,
            datacenters: Vec::new(),
            is_up: false,
            scylla_version,
        }
    }

    pub(crate) fn add_datacenter(&mut self, datacenter: Datacenter) {
        self.datacenters.push(datacenter);
    }

    pub(crate) fn nodes(&self) -> Vec<&Node> {
        self.datacenters
            .iter()
            .flat_map(|dc| dc.racks().iter())
            .flat_map(|rack| rack.nodes().iter())
            .collect()
    }

    pub(crate) fn node_mut(
        &mut self,
        datacenter_idx: usize,
        rack_idx: usize,
        node_idx: usize,
    ) -> Option<&mut Node> {
        self.datacenters
            .get_mut(datacenter_idx)?
            .node_mut(rack_idx, node_idx)
    }

    pub(crate) fn nodes_mut(&mut self) -> Vec<&mut Node> {
        self.datacenters
            .iter_mut()
            .flat_map(|dc| dc.racks_mut())
            .flat_map(|rack| rack.nodes_mut())
            .collect()
    }

    // This function will be used during testing load balancing when starting proxy on each node.
    // It will make drivers requests to go through proxy,
    // that will be running on the same address as node but different port.
    pub(crate) fn update_all_nodes_port(&mut self, port: u16) {
        for node in self.nodes_mut() {
            node.alternator_port = port;
        }
    }

    pub(crate) fn datacenters(&self) -> &[Datacenter] {
        &self.datacenters
    }

    pub(crate) fn datacenters_mut(&mut self) -> &mut [Datacenter] {
        &mut self.datacenters
    }
}
