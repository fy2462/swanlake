use std::sync::Arc;

use crate::config::ServerConfig;
use crate::duckdb::DuckDbEngine;
use crate::service::SwanFlightSqlService;
use anyhow::{Context, Result};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

mod config;
mod duckdb;
mod error;
mod service;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let config = ServerConfig::load().context("failed to load configuration")?;
    info!("service config:\n{:?}", config);
    let addr = config
        .bind_addr()
        .context("failed to resolve bind address")?;

    let engine = Arc::new(DuckDbEngine::new(&config).context("failed to initialize DuckDB")?);
    let flight_service = SwanFlightSqlService::new(engine);

    info!(%addr, "starting SwanDB Flight SQL server");

    Server::builder()
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(flight_service))
        .serve(addr)
        .await
        .context("Flight SQL server terminated unexpectedly")
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,swandb::service=debug,swandb::duckdb=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .compact()
        .init();
}
