use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, LinkedList},
    hash::Hash,
    ops::Deref,
    rc::Rc,
    todo,
};

use common_game::{
    components::resource::{
        BasicResource, BasicResourceType, ComplexResource, ComplexResourceRequest,
        ComplexResourceType,
        GenericResource::{self, ComplexResources},
        ResourceType,
    },
    protocols::{
        orchestrator_explorer::*,
        planet_explorer::{ExplorerToPlanet, PlanetToExplorer},
    },
    utils::ID,
};
use crossbeam_channel::{Receiver, Sender};
use explorer_common::{AiReturn, Bag, BagContent};
use explorer_common::{Explorer as ExplorerTrait, logged_channel::LoggedChannel};
use log::info;

#[derive(Clone, Debug)]
struct PlanetNode(Rc<RefCell<Planet>>);

impl Hash for PlanetNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Rc::as_ptr(&self.0).hash(state);
    }
}

impl PartialEq for PlanetNode {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for PlanetNode {}

impl Deref for PlanetNode {
    type Target = RefCell<Planet>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PlanetNode {
    fn new(planet: Planet) -> Self {
        Self(Rc::new(RefCell::new(planet)))
    }

    /// Connect 2 planet nodes
    fn connect(&mut self, other: &Self) {
        self.borrow_mut().neighbors.insert(other.clone());
        other.borrow_mut().neighbors.insert(self.clone());
    }
}

struct Planet {
    id: ID,
    neighbors: HashSet<PlanetNode>,
    info: Option<PlanetInfo>,
}

impl std::fmt::Debug for Planet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Planet")
            .field("id", &self.id)
            .field(
                "neighbors",
                &self
                    .neighbors
                    .iter()
                    .map(|node| node.borrow().id)
                    .collect::<Vec<_>>(),
            )
            .field("info", &self.info)
            .finish()
    }
}

impl Planet {
    fn new(id: ID, neighbors: HashSet<PlanetNode>, info: Option<PlanetInfo>) -> Self {
        Self {
            id,
            neighbors,
            info,
        }
    }

    fn new_unexplored(id: ID) -> Self {
        Self::new(id, HashSet::new(), None)
    }

    fn is_explored(&self) -> bool {
        self.info.is_some()
    }
}

#[derive(Debug, Clone)]
struct PlanetInfo {
    generates: HashSet<BasicResourceType>,
    produces: HashSet<ComplexResourceType>,
}

#[derive(Clone, Copy)]
enum OrchestratorRequest {
    TravelToPlanet(ID),
    GetNeighbors,
}

enum OrchestratorResponse {
    TravelToPlanetResult(Option<Sender<ExplorerToPlanet>>),
    NeighborsResponse(Vec<ID>),
}

enum PlanetRequest {
    SupportedResources,
    SupportedCombination,
    GenerateResource(BasicResourceType),
    CombineResource(ComplexResourceRequest),
    AvailableEnergyCell,
}

enum PlanetResponse {
    SupportedCombination(HashSet<ComplexResourceType>),
    SupportedResources(HashSet<BasicResourceType>),
    GeneratedResource(Option<BasicResource>),
    CombinedResource(Result<ComplexResource, (String, GenericResource, GenericResource)>),
    AvailableEnergyCells(u32),
}

///A connected graph of planets
struct PlanetMap(HashMap<ID, PlanetNode>);

impl PlanetMap {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn remove(&mut self, key: &ID) -> Option<PlanetNode> {
        let this_planet = match self.0.remove(key) {
            Some(planet) => planet,
            None => return None,
        };

        for neighbor in &this_planet.borrow().neighbors {
            let _: Vec<_> = neighbor
                .borrow_mut()
                .neighbors
                .extract_if(|val| val == &this_planet)
                .collect();
        }
        Some(this_planet)
    }

    /// Insert a new planet into the map and propagate it's adjacencies.
    fn insert(&mut self, planet: Planet) {
        let planet_id = planet.id;
        let planet = PlanetNode::new(planet);
        for neighbor in &planet.borrow().neighbors {
            neighbor.borrow_mut().neighbors.insert(planet.clone());
        }
        self.0.insert(planet_id, planet);
    }

    fn get(&self, key: &ID) -> Option<&PlanetNode> {
        self.0.get(key)
    }

    /// Try to compute the shortest path to a node that matches the conditions in op.
    /// The path is inclusive of `start` and `end`
    fn find_path<F: FnMut(&PlanetNode) -> bool>(
        start: &PlanetNode,
        op: F,
    ) -> Option<LinkedList<PlanetNode>> {
        fn compute_path<F: FnMut(&PlanetNode) -> bool>(
            prev_nodes: LinkedList<PlanetNode>,
            current_node: &PlanetNode,
            mut op: F,
        ) -> Option<LinkedList<PlanetNode>> {
            let mut solution: Option<LinkedList<PlanetNode>> = None;
            for neighbor in &current_node.borrow().neighbors {
                //We don't want loops
                if prev_nodes.contains(&neighbor) {
                    continue;
                }

                let mut current_path = prev_nodes.clone();
                current_path.push_back(neighbor.clone());

                if op(neighbor) {
                    return Some(current_path);
                }
                solution = match (solution, compute_path(current_path, neighbor, &mut op)) {
                    (Some(prev_solution), Some(new_solution)) => {
                        if new_solution.len() < prev_solution.len() {
                            Some(new_solution)
                        } else {
                            Some(prev_solution)
                        }
                    }
                    (None, Some(new_solution)) => Some(new_solution),
                    (prev_solution, None) => prev_solution,
                };
            }
            solution
        }

        compute_path(LinkedList::new(), start, op)
    }

    fn find_path_to_node(start: &PlanetNode, end: &PlanetNode) -> Option<LinkedList<PlanetNode>> {
        PlanetMap::find_path(start, |node| node == end)
    }
}

impl Drop for PlanetMap {
    fn drop(&mut self) {
        for (_, planet) in &mut self.0 {
            //Clear the references amongst planets before dropping the map
            planet.borrow_mut().neighbors = HashSet::new();
        }
    }
}

pub struct Explorer {
    id: ID,
    bag: Bag,
    map: PlanetMap,
    current_planet: PlanetNode,
    auto_mode: bool,
    planet_channel: LoggedChannel<ExplorerToPlanet, PlanetToExplorer>,
    orchestrator_channel: LoggedChannel<ExplorerToOrchestrator<BagContent>, OrchestratorToExplorer>,
}

impl Explorer {
    fn send_orchestrator_request(
        &self,
        request: OrchestratorRequest,
    ) -> Result<OrchestratorResponse, AiReturn> {
        self.orchestrator_channel
            .send(match request {
                OrchestratorRequest::TravelToPlanet(destination_planet_id) => {
                    ExplorerToOrchestrator::TravelToPlanetRequest {
                        explorer_id: self.id,
                        current_planet_id: self.current_planet.borrow().id,
                        dst_planet_id: destination_planet_id,
                    }
                }
                OrchestratorRequest::GetNeighbors => ExplorerToOrchestrator::NeighborsRequest {
                    explorer_id: self.id,
                    current_planet_id: self.current_planet.borrow().id,
                },
            })
            .map_err(|_| AiReturn::Kill)?;

        match self.orchestrator_channel.recv() {
            Ok(OrchestratorToExplorer::MoveToPlanet {
                sender_to_new_planet,
                planet_id: _,
            }) => Ok(OrchestratorResponse::TravelToPlanetResult(
                sender_to_new_planet,
            )),
            Ok(OrchestratorToExplorer::NeighborsResponse { neighbors }) => {
                Ok(OrchestratorResponse::NeighborsResponse(neighbors))
            }
            Ok(OrchestratorToExplorer::StopExplorerAI) => Err(AiReturn::Stop),
            Ok(OrchestratorToExplorer::ResetExplorerAI) => Err(AiReturn::Reset),
            Ok(OrchestratorToExplorer::KillExplorer) => Err(AiReturn::Kill),
            Ok(_) => Err(AiReturn::Kill),
            Err(_) => Err(AiReturn::Kill),
        }
    }

    fn neighbors_request(&self) -> Result<Vec<ID>, AiReturn> {
        match self.send_orchestrator_request(OrchestratorRequest::GetNeighbors)? {
            OrchestratorResponse::NeighborsResponse(neighbors) => Ok(neighbors),
            _ => Err(AiReturn::Kill),
        }
    }

    fn travel_to_planet_request(&mut self, planet: PlanetNode) -> Result<bool, AiReturn> {
        let planet_id = planet.borrow().id;
        match self.send_orchestrator_request(OrchestratorRequest::TravelToPlanet(planet_id))? {
            OrchestratorResponse::TravelToPlanetResult(Some(new_channel)) => {
                self.planet_channel.set_sender(new_channel);
                self.current_planet = planet;
                self.orchestrator_channel
                    .send(ExplorerToOrchestrator::MovedToPlanetResult {
                        explorer_id: self.id,
                        planet_id,
                    })
                    .map_err(|_| AiReturn::Kill)?;
                Ok(true)
            }
            OrchestratorResponse::TravelToPlanetResult(None) => {
                self.map.remove(&planet_id);
                self.orchestrator_channel
                    .send(ExplorerToOrchestrator::MovedToPlanetResult {
                        explorer_id: self.id,
                        planet_id: self.current_planet.borrow().id,
                    })
                    .map_err(|_| AiReturn::Kill)?;
                Ok(false)
            }
            _ => Err(AiReturn::Kill),
        }
    }

    fn send_planet_request(&self, request: PlanetRequest) -> Result<PlanetResponse, AiReturn> {
        self.planet_channel
            .send(match request {
                PlanetRequest::SupportedResources => ExplorerToPlanet::SupportedResourceRequest {
                    explorer_id: self.id,
                },
                PlanetRequest::SupportedCombination => {
                    ExplorerToPlanet::SupportedCombinationRequest {
                        explorer_id: self.id,
                    }
                }
                PlanetRequest::GenerateResource(resource) => {
                    ExplorerToPlanet::GenerateResourceRequest {
                        explorer_id: self.id,
                        resource,
                    }
                }
                PlanetRequest::CombineResource(msg) => ExplorerToPlanet::CombineResourceRequest {
                    explorer_id: self.id,
                    msg,
                },
                PlanetRequest::AvailableEnergyCell => {
                    ExplorerToPlanet::AvailableEnergyCellRequest {
                        explorer_id: self.id,
                    }
                }
            })
            .map_err(|_| AiReturn::Kill)?;

        match self.planet_channel.recv() {
            Ok(PlanetToExplorer::SupportedResourceResponse { resource_list }) => {
                Ok(PlanetResponse::SupportedResources(resource_list))
            }
            Ok(PlanetToExplorer::SupportedCombinationResponse { combination_list }) => {
                Ok(PlanetResponse::SupportedCombination(combination_list))
            }
            Ok(PlanetToExplorer::GenerateResourceResponse { resource }) => {
                Ok(PlanetResponse::GeneratedResource(resource))
            }
            Ok(PlanetToExplorer::CombineResourceResponse { complex_response }) => {
                Ok(PlanetResponse::CombinedResource(complex_response))
            }
            Ok(PlanetToExplorer::AvailableEnergyCellResponse { available_cells }) => {
                Ok(PlanetResponse::AvailableEnergyCells(available_cells))
            }
            Ok(PlanetToExplorer::Stopped) => Err(AiReturn::Stop),
            Err(_) => Err(AiReturn::Kill),
        }
    }

    fn get_currently_available_energy_cells(&self) -> Result<u32, AiReturn> {
        match self.send_planet_request(PlanetRequest::AvailableEnergyCell)? {
            PlanetResponse::AvailableEnergyCells(cells) => Ok(cells),
            _ => Err(AiReturn::Kill),
        }
    }

    fn explore_current_planet(&mut self) -> Result<(), AiReturn> {
        //Get planet information
        let generates = match self.send_planet_request(PlanetRequest::SupportedResources)? {
            PlanetResponse::SupportedResources(resources) => Ok(resources),
            _ => Err(AiReturn::Kill),
        }?;
        let produces = match self.send_planet_request(PlanetRequest::SupportedCombination)? {
            PlanetResponse::SupportedCombination(resources) => Ok(resources),
            _ => Err(AiReturn::Kill),
        }?;

        //Save all the gathered intel
        self.current_planet.borrow_mut().info = Some(PlanetInfo {
            generates,
            produces,
        });

        //Get neighbor information
        let neighbors_ids = self.neighbors_request()?;
        for neighbor_id in neighbors_ids {
            match self.map.get(&neighbor_id) {
                Some(known_planet) => {
                    self.current_planet.connect(known_planet);
                }
                None => self.map.insert(Planet::new(
                    neighbor_id,
                    HashSet::from([self.current_planet.clone()]),
                    None,
                )),
            };
        }
        Ok(())
    }

    fn traverse_path(&mut self, path: LinkedList<PlanetNode>) -> Result<bool, AiReturn> {
        for step in path.into_iter().skip(1) {
            if !self.travel_to_planet_request(step)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn get_prerequisites(resource: ComplexResourceType) -> HashMap<ResourceType, usize> {
    match resource {
        ComplexResourceType::Water => [
            (ResourceType::Basic(BasicResourceType::Oxygen), 1),
            (ResourceType::Basic(BasicResourceType::Hydrogen), 1),
        ]
        .into(),
        ComplexResourceType::Diamond => {
            [(ResourceType::Basic(BasicResourceType::Carbon), 2)].into()
        }
        ComplexResourceType::Life => [
            (ResourceType::Basic(BasicResourceType::Carbon), 1),
            (ResourceType::Complex(ComplexResourceType::Water), 1),
        ]
        .into(),
        ComplexResourceType::Dolphin => [
            (ResourceType::Complex(ComplexResourceType::Water), 1),
            (ResourceType::Complex(ComplexResourceType::Life), 1),
        ]
        .into(),
        ComplexResourceType::Robot => [
            (ResourceType::Complex(ComplexResourceType::Life), 1),
            (ResourceType::Basic(BasicResourceType::Silicon), 1),
        ]
        .into(),
        ComplexResourceType::AIPartner => [
            (ResourceType::Complex(ComplexResourceType::Robot), 1),
            (ResourceType::Complex(ComplexResourceType::Diamond), 1),
        ]
        .into(),
    }
}

impl ExplorerTrait for Explorer {
    fn new(
        id: ID,
        bag: Bag,
        planet_id: ID,
        planet_channel: LoggedChannel<ExplorerToPlanet, PlanetToExplorer>,
        orchestrator_channel: LoggedChannel<
            ExplorerToOrchestrator<BagContent>,
            OrchestratorToExplorer,
        >,
    ) -> Self {
        let current_planet = PlanetNode::new(Planet::new_unexplored(planet_id));
        let map = PlanetMap(HashMap::from([(planet_id, current_planet.clone())]));
        Self {
            id,
            bag,
            map,
            current_planet,
            auto_mode: false,
            planet_channel,
            orchestrator_channel,
        }
    }

    fn get_id(&self) -> ID {
        self.id
    }

    fn get_bag(&mut self) -> &mut Bag {
        &mut self.bag
    }

    fn get_planet_id(&self) -> ID {
        self.current_planet.borrow().id
    }

    fn set_planet_id(&mut self, new: ID) {
        self.current_planet = self
            .map
            .get(&new)
            .cloned()
            .unwrap_or(PlanetNode::new(Planet::new_unexplored(new)));
    }

    fn get_auto_mode(&self) -> bool {
        self.auto_mode
    }

    fn set_auto_mode(&mut self, mode: bool) {
        self.auto_mode = mode;
    }

    fn get_planet_channel(&self) -> LoggedChannel<ExplorerToPlanet, PlanetToExplorer> {
        self.planet_channel.clone()
    }
    fn set_planet_channel_tx(&mut self, tx: Sender<ExplorerToPlanet>) {
        self.planet_channel.set_sender(tx);
    }
    fn set_planet_channel_rx(&mut self, rx: Receiver<PlanetToExplorer>) {
        self.planet_channel.set_receiver(rx);
    }

    fn get_orchestrator_channel(
        &self,
    ) -> LoggedChannel<ExplorerToOrchestrator<BagContent>, OrchestratorToExplorer> {
        self.orchestrator_channel.clone()
    }
    fn set_orchestrator_channel_tx(&mut self, tx: Sender<ExplorerToOrchestrator<BagContent>>) {
        self.orchestrator_channel.set_sender(tx);
    }
    fn set_orchestrator_channel_rx(&mut self, rx: Receiver<OrchestratorToExplorer>) {
        self.orchestrator_channel.set_receiver(rx);
    }

    fn explorer_ai(&mut self) -> explorer_common::AiReturn {
        //Exploration loop.
        //Explore until the entire galaxy is explored.
        fn explore_recursive(
            explorer: &mut Explorer,
            source_planet: Option<PlanetNode>,
        ) -> Result<(), AiReturn> {
            loop {
                //I explore every time just in case something changed
                explorer.explore_current_planet()?;
                let mut all_explored = true;
                let initial_planet = explorer.current_planet.clone();
                for neighbor in &explorer.current_planet.clone().borrow().neighbors {
                    if !neighbor.borrow().is_explored() {
                        if explorer.travel_to_planet_request(neighbor.clone())? {
                            log::info!("Found unexplored planet {}", neighbor.borrow().id);
                            all_explored = false;
                            break;
                        } else {
                            log::error!("Planet {} refused move, ignoring.", neighbor.borrow().id)
                        };
                    }
                }
                if all_explored {
                    match source_planet {
                        Some(source_planet) => {
                            log::info!(
                                "All planets explored here, backtracking to {}",
                                source_planet.borrow().id
                            );

                            explorer.travel_to_planet_request(source_planet)?;
                        }
                        None => {
                            log::info!("All planets in the galaxy explored.",);
                        }
                    }

                    return Ok(());
                } else {
                    explore_recursive(explorer, Some(initial_planet))?;
                }
            }
        }

        //Reset the map. It is probable a lot has changed if we have been paused.
        let current_plent_id = self.current_planet.borrow().id;
        self.map = PlanetMap(HashMap::from([(
            current_plent_id,
            self.current_planet.clone(),
        )]));

        if let Err(err) = explore_recursive(self, None) {
            return err;
        }

        //Now that we have explored the map, we may have fun
        let wanted_resources = [
            (ResourceType::Basic(BasicResourceType::Carbon), 10),
            (ResourceType::Basic(BasicResourceType::Hydrogen), 10),
            (ResourceType::Basic(BasicResourceType::Oxygen), 10),
            (ResourceType::Basic(BasicResourceType::Silicon), 10),
        ];

        //Travel to the nearest planet that is of use
        let mut resource_to_act_on: Option<ResourceType> = None;
        let path = PlanetMap::find_path(&self.current_planet, |planet| {
            for (wanted, amount) in wanted_resources {
                if self.bag.contains(wanted) < amount
                    && planet.borrow().info.clone().is_some_and(|p_info| {
                        match wanted {
                            ResourceType::Basic(basic_resource_type) => {
                                p_info.generates.contains(&basic_resource_type)
                            }
                            ResourceType::Complex(complex_resource_type) => {
                                //Ensure we can actually build a complex resource
                                p_info.produces.contains(&complex_resource_type) && {
                                    get_prerequisites(complex_resource_type).iter().all(
                                        |(prerequisite, amount)| {
                                            self.bag.contains(*prerequisite) > *amount
                                        },
                                    )
                                }
                            }
                        }
                    })
                {
                    resource_to_act_on = Some(wanted);
                    return true;
                }
            }
            false
        });

        match path {
            Some(path) => {
                info!(
                    "Selected path {:?}",
                    path.iter().map(|step| step.borrow().id)
                );
                if let Err(err) = self.traverse_path(path) {
                    return err;
                };
            }
            None => info!("No path selected"),
        }

        AiReturn::Kill
    }

    fn reset(&mut self) {
        self.bag = Bag::new();
        self.map = PlanetMap(HashMap::from([(
            self.current_planet.borrow().id,
            self.current_planet.clone(),
        )]));
    }
}

// The tested functions were moved to explorer_common
#[cfg(test)]
mod tests {
    use common_game::logging::ActorType::Orchestrator;

    fn run_orchestrator_interactive() {}
}
