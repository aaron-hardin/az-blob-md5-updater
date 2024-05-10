use std::{collections::VecDeque, sync::Arc};

use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use clap::Parser;
use futures::stream::StreamExt;
use tokio::sync::mpsc::{self, Sender};

// Import the base64 crate Engine trait anonymously so we can
// call its methods without adding to the namespace.
use base64::engine::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

/// A CLI for checking md5 in blob storage
#[derive(Debug, Parser)]
#[clap(name = "blob-md5-updater")]
#[clap(about = "A CLI for checking md5 in blob storage", long_about = None)]
struct Cli {
	#[clap(short, long)]
	account: String,
	#[clap(short, long)]
	sas_token: String,
	#[clap(short, long)]
	container_name: String,
	#[clap(long)]
	fixit: bool,
	#[clap(long, default_value="1024")]
	chunk_size_kb: u64,
	#[clap(long)]
	root: Option<String>,
	#[clap(long)]
	experimental_threads: bool,
}

#[tokio::main]
async fn main() -> azure_core::Result<()> {
	let args = Cli::parse();

	let console_filter: tracing_subscriber::EnvFilter = "trace,azure_core=warn,azure_storage=warn,hyper_util=warn".into();
	let file_filter: tracing_subscriber::EnvFilter = "info,azure_core=warn,azure_storage=warn,hyper_util=warn".into();
	let console_log = tracing_subscriber::fmt::layer()
		.with_ansi(true)
		.with_writer(std::io::stderr.with_min_level(tracing::Level::WARN).or_else(std::io::stdout))
		.with_filter(console_filter);
	let file_appender = RollingFileAppender::builder()
		.rotation(Rotation::HOURLY)
		.filename_prefix("run")
		.filename_suffix("log")
		.build("log")
		.map_err(|err| azure_core::Error::new(azure_core::error::ErrorKind::Other, err))?;
	let (nb, _g) = tracing_appender::non_blocking(file_appender);
	let file_log = tracing_subscriber::fmt::layer()
		.with_writer(nb)
		.with_ansi(false)
		.with_filter(file_filter);
	let subscriber = tracing_subscriber::registry()
		.with(console_log)
		.with(file_log);
	tracing::subscriber::set_global_default(subscriber)
		.map_err(|err| azure_core::Error::new(azure_core::error::ErrorKind::Other, err))?;

	let account = args.account;
	let sas_token = args.sas_token;
	let container_name = args.container_name;
	let root = args.root;

	tracing::info!("getting blob service client");

	let storage_credentials = StorageCredentials::sas_token(sas_token)?;
	let blob_service_client = BlobServiceClient::new(account, storage_credentials);

	tracing::info!("getting container client");

	let container_client = Arc::new(blob_service_client.container_client(&container_name));

	// Create a simple streaming channel
	let (tx, mut rx) = mpsc::channel(100);

	tracing::info!("Starting at {root:?}");
	let mut list_blob_resp = match root.as_ref() {
		Some(root) => container_client.list_blobs().prefix(root.clone()).delimiter("/").into_stream(),
		None => container_client.list_blobs().delimiter("/").into_stream()
	};
	
	while let Some(value) = list_blob_resp.next().await {
		if value.is_err() {
			tracing::error!("Err for {root:?} {:?}", value.err());
			break;
		}
		let blob_response = value.unwrap();

		// Iterate down further
		for blob_prefix in blob_response.blobs.prefixes() {
			start_blob_thread(container_client.clone(), tx.clone(), blob_prefix.name.clone());
		}
	}

	// drop the original tx so that it doesn't hold up the rx
	drop(tx);

	// Starts 5 'threads' to handle the actual calculation
	if args.experimental_threads {
		println!("started experimental_threads");
		let mut tasks: [tokio::task::JoinHandle<azure_core::Result<()>>; 5] = [tokio::spawn(async {Ok(())}), tokio::spawn(async {Ok(())}), tokio::spawn(async {Ok(())}), tokio::spawn(async {Ok(())}), tokio::spawn(async {Ok(())})];

		let mut count = 0u32;
		loop {
			let mut maybe_blob = rx.recv().await;
			if maybe_blob.is_some() {
				tracing::info!("No MD5 -- {}", maybe_blob.as_ref().unwrap().name);
				count += 1;
				if count % 100 == 0 {
					tracing::trace!("{count} -- with no MD5");
				}

				if args.fixit {
					// wait for an available thread to take it
					let mut waiting_for_thread = true;
					while waiting_for_thread {
						for i in 0..5 {
							if tasks[i].is_finished() {
								let container_client = container_client.clone();
								let blob = maybe_blob.take().unwrap();
								let mut new_handle = tokio::spawn(async move {
									let blob_client = container_client
										.blob_client(blob.name.clone());

									// TODO: revisit stream size
									let mut stream = Box::pin(blob_client.get().chunk_size(1024u64 * args.chunk_size_kb).into_stream());
									let mut md5context = md5::Context::new();
									{
										while let Some(value) = stream.next().await {
											let value = value?.data.collect().await?.to_vec();
											md5context.consume(value);
										}
									}
									let md5digest = md5context.compute().0;
									tracing::info!("Computed: {:?} for {}", BASE64.encode(md5digest), blob.name);

									let result = blob_client
										.set_properties()
										.set_from_blob_properties(blob.properties)
										.content_md5(md5digest)
										.into_future()
										.await;

									if result.is_err() {
										tracing::error!("Failed to update md5 for {} -- {:?}", blob.name, result.err());
									}

									Ok(())
								});
								std::mem::swap(&mut tasks[i], &mut new_handle);
								waiting_for_thread = false;
								break;
							}
						}

						if waiting_for_thread {
							// sleep a little
							tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
						}
					}
				}
			} else {
				// no more to rx
				break;
			}
		}

		// wait for the final tasks to finish
		for task in tasks {
			if let Err(err) = task.await {
				println!("Error on task {err}");
			}
		}

		tracing::info!("Main thread done - found {count} total with no MD5");
	} else {
		let mut count = 0u32;
		while let Some(blob) = rx.recv().await {
			tracing::info!("No MD5 -- {}", blob.name);
			count += 1;
			if count % 100 == 0 {
				tracing::trace!("{count} -- with no MD5");
			}

			if args.fixit {
				let blob_client = container_client
					.blob_client(blob.name.clone());

				// TODO: revisit stream size
				let mut stream = Box::pin(blob_client.get().chunk_size(1024u64 * args.chunk_size_kb).into_stream());
				let mut md5context = md5::Context::new();
				{
					while let Some(value) = stream.next().await {
						let value = value?.data.collect().await?.to_vec();
						md5context.consume(value);
					}
				}
				let md5digest = md5context.compute().0;
				tracing::info!("Computed: {:?} for {}", BASE64.encode(md5digest), blob.name);

				let result = blob_client
					.set_properties()
					.set_from_blob_properties(blob.properties)
					.content_md5(md5digest)
					.into_future()
					.await;

				if result.is_err() {
					tracing::error!("Failed to update md5 for {} -- {:?}", blob.name, result.err());
				}
			}
		}
		tracing::info!("Main thread done - found {count} total with no MD5");
	}

	Ok(())
}

fn start_blob_thread(container_client: Arc<ContainerClient>, tx: Sender<Blob>, starting_prefix: String) {
	tokio::spawn(async move {
		// This thread will handle getting blobs and send them to the main thread for processing
		process_blob(container_client, tx, starting_prefix).await;
	});
}

async fn process_blob(container_client: Arc<ContainerClient>, tx: Sender<Blob>, starting_prefix: String) {
	let mut queue = VecDeque::from([starting_prefix.clone()]);
	let mut count = 0u32;
	while let Some(item) = queue.pop_front() {
		if has_less_than(&item, '/', 4) {
			tracing::trace!("{item}");
		}
		count += 1;
		if count % 200 == 0 {
			tracing::trace!("{starting_prefix} -- {count}");
		}
		let mut list_blob_resp = container_client
			.list_blobs()
			.prefix(item.clone())
			.delimiter("/")
			.into_stream();
		
		while let Some(value) = list_blob_resp.next().await {
			if value.is_err() {
				tracing::error!("Err for {item} {:?}", value.err());
				break;
			}
			let blob_response = value.unwrap();

			// Send blobs to other thread for processing
			for b in blob_response.blobs.blobs() {
				if b.properties.content_md5.is_none() {
					tx.send(b.clone()).await.unwrap();
				}
			}

			// Iterate down further
			for blob_prefix in blob_response.blobs.prefixes() {
				queue.push_back(blob_prefix.name.clone());
			}
		}
	}

	tracing::info!("{starting_prefix} -- total folder count = {count} -- DONE");
}

fn has_less_than(s: &str, c: char, mut count: i32) -> bool {
	for cc in s.chars() {
		if cc == c {
			count -= 1;
			if count == 0 {
				return false;
			}
		}
	}

	true
}

#[cfg(test)]
mod unit_tests {
	use super::*;

	#[test]
	fn test_has_less_than() {
		assert_eq!(true, has_less_than("UploadFiles/Folder1/@CSVs/", '/', 4));
		assert_eq!(false, has_less_than("UploadFiles/Folder1/@CSVs/1-Unprocessed/", '/', 4));
	}
}
