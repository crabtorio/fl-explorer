use std::{
    collections::{HashMap, HashSet},
    todo,
};

use common_game::{
    components::resource::{BasicResource, BasicResourceType, ComplexResourceType},
    protocols::{
        orchestrator_explorer::*,
        planet_explorer::{ExplorerToPlanet, PlanetToExplorer},
    },
    utils::ID,
};
use crossbeam_channel::{Receiver, Sender};
use explorer_common::{AiReturn, Bag, BagContent};
use explorer_common::{Explorer as ExplorerTrait, logged_channel::LoggedChannel};
pub struct Explorer {
    id: ID,
    bag: Bag,
    current_planet_id: ID,
    auto_mode: bool,
    planet_channel: LoggedChannel<ExplorerToPlanet, PlanetToExplorer>,
    orchestrator_channel: LoggedChannel<ExplorerToOrchestrator<BagContent>, OrchestratorToExplorer>,
    known_planets: HashMap<ID, PlanetInfo>,
}
struct PlanetInfo {
    id: ID,
    adjacent_planets: Vec<ID>,
    supported_resources: HashSet<BasicResourceType>,
    supported_combinations: HashSet<ComplexResourceType>,
}

impl PlanetInfo {
    fn new(
        id: ID,
        adjacent_planets: &[ID],
        supported_resources: HashSet<BasicResourceType>,
        supported_combinations: HashSet<ComplexResourceType>,
    ) -> Self {
        Self {
            id,
            supported_resources,
            supported_combinations,
            adjacent_planets: Vec::from(adjacent_planets),
        }
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
        Self {
            id,
            bag,
            current_planet_id: planet_id,
            auto_mode: false,
            planet_channel,
            orchestrator_channel,
            known_planets: HashMap::new(),
        }
    }

    fn get_id(&self) -> ID {
        self.id
    }

    fn get_bag(&mut self) -> &mut Bag {
        &mut self.bag
    }

    fn get_planet_id(&self) -> ID {
        self.current_planet_id
    }

    fn set_planet_id(&mut self, new: ID) {
        self.current_planet_id = new;
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
        todo!();
    }

    fn reset(&mut self) {
        todo!()
    }
}

// The tested functions were moved to explorer_common
#[cfg(test)]
mod tests {}
