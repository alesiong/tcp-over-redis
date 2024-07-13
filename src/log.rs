use std::fs::File;
use std::str::FromStr;
use tracing::Level;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use crate::config::CommonConfig;
use crate::error::ProxyError;

pub fn init_log(common: &CommonConfig) -> Result<(), ProxyError> {
    let mut layers = vec![];
    let level = Level::from_str(&common.log_level)?;
    if !common.log_path.is_empty() {
        layers.push(
            tracing_subscriber::fmt::layer()
                .with_writer(File::options().create(true).append(true).open(&common.log_path)?)
                .with_filter(LevelFilter::from(level))
                .boxed()
        );
        layers.push(tracing_subscriber::fmt::layer()
            .with_filter(LevelFilter::ERROR)
            .boxed()
        );
    } else {
        layers.push(tracing_subscriber::fmt::layer()
            .with_filter(LevelFilter::from(level))
            .boxed()
        );
    }


    tracing_subscriber::registry()
        .with(layers)
        .init();

    Ok(())
}