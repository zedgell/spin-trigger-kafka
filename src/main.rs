use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::Parser;
use is_terminal::IsTerminal;
use kafka::client::GroupOffsetStorage;
use kafka::consumer::{Consumer, FetchOffset};
use serde::{Deserialize, Serialize};
use spin_kafka::SpinKafkaData;
use spin_trigger::{
    cli::{NoArgs, TriggerExecutorCommand},
    TriggerAppEngine, TriggerExecutor,
};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

wit_bindgen_wasmtime::import!({paths: ["spin_kafka.wit"], async: *});

pub(crate) type RuntimeData = SpinKafkaData;

type Command = TriggerExecutorCommand<KafkaTrigger>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(std::io::stderr().is_terminal())
        .init();

    let t = Command::parse();
    t.run().await
}

pub struct KafkaTrigger {
    engine: TriggerAppEngine<Self>,
    kafka_components: Vec<Component>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaTriggerConfig {
    pub component: String,
    pub broker_urls: String,
    pub topic: String,
    pub group: String,
    pub offset: String,
}

#[derive(Clone, Debug)]
struct Component {
    pub id: String,
    pub broker_urls: String,
    pub topic: String,
    pub group: String,
    pub offset: Offset,
}

#[derive(Clone, Debug)]
pub enum Offset {
    Earliest,
    Latest,
}

impl FromStr for Offset {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Earliest" => Ok(Offset::Earliest),
            "Latest" => Ok(Offset::Latest),
            _ => Err(Error::msg("Accepted Values Are: Earliest, Latest")),
        }
    }
}

// This is a placeholder - we don't yet detect any situations that would require
// graceful or ungraceful exit.  It will likely require rework when we do.  It
// is here so that we have a skeleton for returning errors that doesn't expose
// us to thoughtlessly "?"-ing away an Err case and creating a situation where a
// transient failure could end the trigger.
#[allow(dead_code)]
#[derive(Debug)]
enum TerminationReason {
    ExitRequested,
    Other(String),
}

#[async_trait]
impl TriggerExecutor for KafkaTrigger {
    const TRIGGER_TYPE: &'static str = "kafka";
    type RuntimeData = RuntimeData;
    type TriggerConfig = KafkaTriggerConfig;
    type RunConfig = NoArgs;

    fn new(engine: TriggerAppEngine<Self>) -> Result<Self> {
        let kafka_components = engine
            .trigger_configs()
            .map(|(_, config)| Component {
                id: config.component.clone(),
                broker_urls: config.broker_urls.clone(),
                topic: config.topic.clone(),
                group: config.group.clone(),
                offset: Offset::from_str(config.offset.as_str()).unwrap_or(Offset::Latest),
            })
            .collect();
        Ok(Self {
            engine,
            kafka_components,
        })
    }

    async fn run(self, _config: Self::RunConfig) -> Result<()> {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            std::process::exit(0);
        });
        let engine = Arc::new(self.engine);
        let mut handlers = vec![];
        for component in self.kafka_components {
            let engine_clone = engine.clone();
            handlers.push(tokio::spawn(async move {
                Self::start_listener(engine_clone, component)
            }))
        }

        let _ = futures::future::join_all(handlers);

        Ok(())
    }
}

impl KafkaTrigger {
    async fn start_listener(engine: Arc<TriggerAppEngine<Self>>, component: Component) {
        let con = Arc::new(Mutex::new(
            Consumer::from_hosts(
                component
                    .broker_urls
                    .split(',')
                    .map(|s| s.to_string())
                    .collect(),
            )
            .with_topic(component.topic.clone())
            .with_group(component.group.clone())
            .with_fallback_offset(match component.offset {
                Offset::Earliest => FetchOffset::Earliest,
                Offset::Latest => FetchOffset::Latest,
            })
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap(),
        ));

        loop {
            let mss = con.lock().unwrap().poll().unwrap();
            for ms in mss.iter() {
                for msg in ms.messages() {
                    let key = String::from_utf8(msg.key.to_vec()).unwrap();

                    let value = String::from_utf8(msg.value.to_vec()).unwrap();

                    let message = spin_kafka::Message {
                        offset: msg.offset as u64,
                        key: key.as_str(),
                        value: value.as_str(),
                    };
                    let processor = KafkaMessageProcessor::new(
                        &engine,
                        con.clone(),
                        &component,
                        ms.partition().clone(),
                    );
                    processor.process_message(message).await;
                }
            }
        }
    }
}

struct KafkaMessageProcessor {
    engine: Arc<TriggerAppEngine<KafkaTrigger>>,
    con: Arc<Mutex<Consumer>>,
    component: Component,
    partition: i32,
}

impl KafkaMessageProcessor {
    fn new(
        engine: &Arc<TriggerAppEngine<KafkaTrigger>>,
        con: Arc<Mutex<Consumer>>,
        component: &Component,
        partition: i32,
    ) -> Self {
        Self {
            engine: engine.clone(),
            con,
            component: component.clone(),
            partition,
        }
    }

    async fn process_message(self, msg: spin_kafka::Message<'_>) {
        let offset = msg.offset;
        tracing::trace!("Offset {offset}: spawned processing task");

        let action = self.execute_wasm(msg.clone()).await;

        match action {
            Ok(spin_kafka::MessageAction::Commit) => {
                tracing::trace!("Offset {offset} processed successfully: action is Commit");
                match self.con.lock().unwrap().consume_message(
                    self.component.topic.as_str(),
                    self.partition,
                    msg.offset as i64,
                ) {
                    Ok(_) => tracing::trace!("Offset {offset} committed"),
                    Err(e) => tracing::error!("Offset {offset}: error committing: {e:?}"),
                };
            }
            Ok(spin_kafka::MessageAction::Leave) => {
                tracing::trace!("Offset {offset} processed successfully: action is Leave")
            }
            Err(e) => tracing::error!("Offset {offset} processing error: {}", e.to_string()),
        }
    }

    async fn execute_wasm(
        &self,
        message: spin_kafka::Message<'_>,
    ) -> Result<spin_kafka::MessageAction> {
        let offset = message.offset;
        let component_id = &self.component.id;
        tracing::trace!("Offset {offset}: executing component {component_id}");
        let (instance, mut store) = self.engine.prepare_instance(component_id).await?;
        let kafka_engine = spin_kafka::SpinKafka::new(&mut store, &instance, |data| data.as_mut())?;
        match kafka_engine.handle_message(&mut store, message).await {
            Ok(Ok(action)) => {
                tracing::trace!("Offset {offset}: component {component_id} completed okay");
                Ok(action)
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "Offset {offset}: component {component_id} returned error {:?}",
                    e
                );
                Err(anyhow::anyhow!(
                    "Component {component_id} returned error processing offset {offset}"
                ))
            }
            Err(e) => {
                tracing::error!(
                    "Offset {offset}: engine error running component {component_id}: {:?}",
                    e
                );
                Err(anyhow::anyhow!(
                    "Error executing component {component_id} while processing offset {offset}"
                ))
            }
        }
    }
}
