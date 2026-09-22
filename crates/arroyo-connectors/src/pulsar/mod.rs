use anyhow::{anyhow, bail};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use typify::import_types;

mod source;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./pulsar.svg");

import_types!(
    schema = "src/pulsar/profile.json",
    convert = {{type = "string", format = "var-str"} = VarStr}
);
import_types!(schema = "src/pulsar/table.json");

pub struct PulsarConnector;

impl PulsarConnector {
    fn connection_from_options(options: &mut ConnectorOptions) -> anyhow::Result<PulsarConfig> {
        let service_url = options.pull_str("serviceUrl")?;
        let namespace = options.pull_opt_str("namespace")?;
        let authentication = match options.pull_opt_str("authentication.type")?.as_deref() {
            Some("Token") | Some("token") => PulsarConfigAuthentication::Token {
                token: VarStr::new(options.pull_str("authentication.token")?),
            },
            Some("Basic") | Some("basic") => PulsarConfigAuthentication::Basic {
                username: VarStr::new(options.pull_str("authentication.username")?),
                password: VarStr::new(options.pull_str("authentication.password")?),
            },
            Some("OAuth2") | Some("oauth2") => PulsarConfigAuthentication::OAuth2 {
                issuer_url: options.pull_str("authentication.issuerUrl")?,
                credentials_url: VarStr::new(options.pull_str("authentication.credentialsUrl")?),
                audience: VarStr::new(options.pull_str("authentication.audience")?),
                scope: options
                    .pull_opt_str("authentication.scope")?
                    .map(VarStr::new),
            },
            Some("None") | Some("none") | None => PulsarConfigAuthentication::None {},
            Some(other) => bail!("unsupported Pulsar authentication type '{other}'"),
        };

        let certificate_chain_file = options
            .pull_opt_str("tls.certificateChainFile")?
            .map(VarStr::new);
        let allow_insecure_connection = options.pull_opt_bool("tls.allowInsecureConnection")?;
        let tls_hostname_verification_enabled =
            options.pull_opt_bool("tls.tlsHostnameVerificationEnabled")?;
        let tls = (certificate_chain_file.is_some()
            || allow_insecure_connection.is_some()
            || tls_hostname_verification_enabled.is_some())
        .then_some(Tls {
            certificate_chain_file,
            allow_insecure_connection,
            tls_hostname_verification_enabled,
        });

        Ok(PulsarConfig {
            service_url,
            namespace,
            authentication,
            tls,
        })
    }

    fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<PulsarTable> {
        Ok(PulsarTable {
            topic: options.pull_str("topic")?,
            subscription: options.pull_str("subscription")?,
            subscription_type: options.pull_opt_str("subscriptionType")?.map(|value| {
                match value.as_str() {
                    "Failover" => SubscriptionType::Failover,
                    "Shared" => SubscriptionType::Shared,
                    "KeyShared" => SubscriptionType::KeyShared,
                    _ => SubscriptionType::Exclusive,
                }
            }),
        })
    }
}

impl Connector for PulsarConnector {
    type ProfileT = PulsarConfig;
    type TableT = PulsarTable;

    fn name(&self) -> &'static str {
        "pulsar"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "pulsar".to_string(),
            name: "Pulsar".to_string(),
            icon: ICON.to_string(),
            description: "Read from an Apache Pulsar topic".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn test_profile(
        &self,
        profile: Self::ProfileT,
    ) -> Option<tokio::sync::oneshot::Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let message = match source::connect(&profile).await {
                Ok(_) => TestSourceMessage::done("Successfully connected to Pulsar"),
                Err(error) => {
                    TestSourceMessage::fail(format!("Failed to connect to Pulsar: {error:#}"))
                }
            };
            let _ = tx.send(message);
        });

        Some(rx)
    }

    fn get_autocomplete(
        &self,
        profile: Self::ProfileT,
    ) -> tokio::sync::oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let namespace = profile
                .namespace
                .clone()
                .ok_or_else(|| anyhow!("Pulsar namespace is required to list topic options"));
            let result = async {
                let namespace = namespace?;
                let pulsar = source::connect(&profile).await?;
                let topics = pulsar
                    .get_topics_of_namespace(
                        namespace.clone(),
                        pulsar::message::proto::command_get_topics_of_namespace::Mode::Persistent,
                    )
                    .await
                    .map_err(|error| {
                        anyhow!(
                            "failed to list Pulsar topics in namespace '{}': {}",
                            namespace,
                            error
                        )
                    })?;
                Ok(HashMap::from([("topic".to_string(), topics)]))
            }
            .await;
            let _ = tx.send(result);
        });

        rx
    }

    fn test(
        &self,
        _: &str,
        config: PulsarConfig,
        table: PulsarTable,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::spawn(async move {
            let message = match source::connect(&config).await {
                Ok(pulsar) => match pulsar.lookup_topic(&table.topic).await {
                    Ok(_) => TestSourceMessage::done(format!(
                        "Successfully connected to Pulsar and resolved topic '{}'",
                        table.topic
                    )),
                    Err(error) => TestSourceMessage::fail(format!(
                        "Connected to Pulsar but failed to resolve topic '{}': {error}",
                        table.topic
                    )),
                },
                Err(error) => {
                    TestSourceMessage::fail(format!("Failed to connect to Pulsar: {error:#}"))
                }
            };
            let _ = tx.send(message).await;
        });
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: PulsarConfig,
        table: PulsarTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let schema = schema
            .ok_or_else(|| anyhow!("no schema defined for Pulsar connection"))?
            .to_owned();
        let format = schema
            .format
            .clone()
            .ok_or_else(|| anyhow!("'format' must be set for Pulsar connection"))?;
        let operator_config = OperatorConfig {
            connection: serde_json::to_value(config)?,
            table: serde_json::to_value(table)?,
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };
        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Source,
            schema,
            &operator_config,
            format!("PulsarSource<{:?}>", operator_config.table),
        ))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let config = match profile {
            Some(p) => serde_json::from_value::<PulsarConfig>(p.config.clone())?,
            None => Self::connection_from_options(options)?,
        };
        let table = Self::table_from_options(options)?;
        self.from_config(None, name, config, table, schema)
    }

    fn make_operator(
        &self,
        profile: PulsarConfig,
        table: PulsarTable,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        Ok(ConstructedOperator::from_source(Box::new(
            source::PulsarSourceFunc {
                config: profile,
                table,
                format: config
                    .format
                    .ok_or_else(|| anyhow!("format required for Pulsar source"))?,
                framing: config.framing,
                bad_data: config.bad_data,
                metadata_fields: config.metadata_fields,
            },
        )))
    }
}
