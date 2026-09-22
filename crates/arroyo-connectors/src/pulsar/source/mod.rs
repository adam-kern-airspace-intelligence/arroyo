use super::{PulsarConfig, PulsarTable};
use anyhow::Context;
use arroyo_operator::SourceFinishType;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::{ControlMessage, connector_err};
use async_trait::async_trait;
use futures::StreamExt;
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};
use tokio::select;

pub struct PulsarSourceFunc {
    pub config: PulsarConfig,
    pub table: PulsarTable,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub metadata_fields: Vec<arroyo_rpc::MetadataField>,
}

pub(super) async fn connect(config: &PulsarConfig) -> anyhow::Result<Pulsar<TokioExecutor>> {
    let mut builder = Pulsar::builder(config.service_url.clone(), TokioExecutor);
    match &config.authentication {
        super::PulsarConfigAuthentication::None {} => {}
        super::PulsarConfigAuthentication::Token { token } => {
            let token = token.sub_env_vars().context("invalid Pulsar token")?;
            builder = builder.with_auth(pulsar::Authentication {
                name: "token".to_string(),
                data: token.into_bytes(),
            });
        }
        super::PulsarConfigAuthentication::Basic { username, password } => {
            let username = username.sub_env_vars().context("invalid Pulsar username")?;
            let password = password.sub_env_vars().context("invalid Pulsar password")?;
            builder = builder.with_auth_provider(
                pulsar::authentication::basic::BasicAuthentication::new(
                    username.as_ref(),
                    password.as_ref(),
                ),
            );
        }
        super::PulsarConfigAuthentication::OAuth2 {
            issuer_url,
            credentials_url,
            audience,
            scope,
        } => {
            let credentials_url = credentials_url
                .sub_env_vars()
                .context("invalid Pulsar OAuth2 credentials URL")?;
            let audience = audience
                .sub_env_vars()
                .context("invalid Pulsar OAuth2 audience")?;
            let scope = scope
                .as_ref()
                .map(|scope| scope.sub_env_vars())
                .transpose()
                .context("invalid Pulsar OAuth2 scope")?;
            builder = builder.with_auth_provider(
                pulsar::authentication::oauth2::OAuth2Authentication::client_credentials(
                    pulsar::authentication::oauth2::OAuth2Params {
                        issuer_url: issuer_url.clone(),
                        credentials_url,
                        audience: Some(audience),
                        scope,
                    },
                ),
            );
        }
    }
    if let Some(tls) = &config.tls {
        if let Some(certificate_chain_file) = &tls.certificate_chain_file {
            let certificate_chain_file = certificate_chain_file
                .sub_env_vars()
                .context("invalid Pulsar certificate chain file")?;
            builder = builder
                .with_certificate_chain_file(certificate_chain_file)
                .context("failed to read Pulsar certificate chain")?;
        }
        if let Some(allow_insecure_connection) = tls.allow_insecure_connection {
            builder = builder.with_allow_insecure_connection(allow_insecure_connection);
        }
        if let Some(tls_hostname_verification_enabled) = tls.tls_hostname_verification_enabled {
            builder =
                builder.with_tls_hostname_verification_enabled(tls_hostname_verification_enabled);
        }
    }

    builder.build().await.context("failed to connect to Pulsar")
}

#[async_trait]
impl SourceOperator for PulsarSourceFunc {
    fn name(&self) -> String {
        format!("pulsar-source-{}", self.table.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("p", "Pulsar source state")
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &self.metadata_fields,
        );
        let pulsar = connect(&self.config).await.map_err(|e| {
            connector_err!(External, WithBackoff, "failed to connect to Pulsar: {}", e)
        })?;
        let subscription_type = match self.table.subscription_type {
            Some(super::SubscriptionType::Failover) => SubType::Failover,
            Some(super::SubscriptionType::Shared) => SubType::Shared,
            Some(super::SubscriptionType::KeyShared) => SubType::KeyShared,
            _ => SubType::Exclusive,
        };
        let mut consumer: Consumer<Vec<u8>, _> = pulsar
            .consumer()
            .with_topic(self.table.topic.clone())
            .with_subscription(self.table.subscription.clone())
            .with_subscription_type(subscription_type)
            .with_consumer_name(format!(
                "{}-{}",
                ctx.task_info.job_id, ctx.task_info.operator_id
            ))
            .build()
            .await
            .map_err(|e| {
                connector_err!(
                    External,
                    WithBackoff,
                    "failed to subscribe to Pulsar topic '{}': {}",
                    self.table.topic,
                    e
                )
            })?;
        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        loop {
            select! {
                message = consumer.next() => match message {
                    Some(Ok(message)) => {
                        let timestamp = message.metadata().event_time
                            .or(Some(message.metadata().publish_time)).unwrap_or_default();
                        let timestamp = UNIX_EPOCH + Duration::from_millis(timestamp);
                        collector.deserialize_slice(&message.payload.data, timestamp, None).await?;
                        consumer.ack(&message).await.map_err(|e| connector_err!(External, WithBackoff, "failed to acknowledge Pulsar message: {}", e))?;
                        if collector.should_flush() { collector.flush_buffer().await?; }
                    }
                    Some(Err(error)) => return Err(connector_err!(External, WithBackoff, "Pulsar consumer error: {}", error)),
                    None => return Ok(SourceFinishType::Graceful),
                },
                _ = flush_ticker.tick() => if collector.should_flush() { collector.flush_buffer().await?; },
                control_message = ctx.control_rx.recv() => match control_message {
                    Some(ControlMessage::Checkpoint(c)) => if self.start_checkpoint(c, ctx, collector).await { return Ok(SourceFinishType::Immediate); },
                    Some(ControlMessage::Stop { mode: StopMode::Graceful }) => return Ok(SourceFinishType::Graceful),
                    Some(ControlMessage::Stop { mode: StopMode::Immediate }) => return Ok(SourceFinishType::Immediate),
                    Some(ControlMessage::LoadCompacted { compacted }) => ctx.load_compacted(compacted).await,
                    Some(ControlMessage::Commit { .. }) => unreachable!("sources shouldn't receive commit messages"),
                    Some(ControlMessage::NoOp) | None => {}
                }
            }
        }
    }
}
