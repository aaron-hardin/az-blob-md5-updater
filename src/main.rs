use core::option::Option::Some;
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
	chunk_size_kb: usize,
	#[clap(long)]
	root: Option<String>,
	#[clap(long, default_value="5")]
	concurrency: u8,
}

#[tokio::main]
async fn main() -> azure_core::Result<()> {
	let args = Cli::parse();

	let console_filter: tracing_subscriber::EnvFilter = "trace,azure_core=warn,azure_storage=warn,hyper_util=warn,typespec_client_core=warn".into();
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

	// TODO: currently using the older azure_storage_blobs and azure_storage_blob, need to migrate to just the newer one
	let cc = azure_storage_blob::clients::BlobContainerClient::from_url(azure_core::Url::parse(&format!("https://{}.blob.core.windows.net/{}?{}", account, container_name, sas_token)).unwrap(), None, None).unwrap();
	let cc = Arc::new(cc);

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

	// TODO: this needs testing/cleanup
	// Starts 'concurrency' (default 5) 'threads' to handle the actual calculation
	println!("started processing");
	let concurrency = args.concurrency as usize;
	//let mut tasks: Vec<tokio::task::JoinHandle<azure_core::Result<()>>> = Vec::with_capacity(concurrency);
	let mut tasks: Vec<tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> = Vec::with_capacity(concurrency);
	//let mut tasks: [tokio::task::JoinHandle<azure_core::Result<()>>; args.concurrency] = [tokio::spawn(async {Ok(())}), tokio::spawn(async {Ok(())}), tokio::spawn(async {Ok(())}), tokio::spawn(async {Ok(())}), tokio::spawn(async {Ok(())})];
	for _i in 0..concurrency {
		tasks.push(tokio::spawn(async {Ok(())}));
	}

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
					for i in 0..concurrency {
						if tasks[i].is_finished() {
							let blob = maybe_blob.take().unwrap();
							let other_container_client = cc.clone();
							let mut new_handle = tokio::spawn(update_md5_for_blob(other_container_client, blob, args.chunk_size_kb));
							std::mem::swap(&mut tasks[i], &mut new_handle);
							waiting_for_thread = false;

							// print error for new_handle (which now has the completed task), if any
							// Note: await is fine here because we know the task is already completed since we checked is_finished() above
							match new_handle.await {
								Ok(Ok(())) => {},
								Ok(Err(err)) => {
									tracing::error!("Error processing blob: {err}");
								},
								Err(err) => {
									tracing::error!("JoinError on task {err}");
								}
							}
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

	tracing::info!("Waiting for tasks to finish...");

	// wait for the final tasks to finish
	for task in tasks {
		match task.await {
			Ok(Ok(())) => {
				//tracing::info!("Task finished");
			},
			Ok(Err(err)) => {
				tracing::error!("Error processing blob: {err}");
			},
			Err(err) => {
				tracing::error!("JoinError on task {err}");
			}
		}
	}

	tracing::info!("Main thread done - found {count} total with no MD5");

	Ok(())
}

fn start_blob_thread(container_client: Arc<ContainerClient>, tx: Sender<Blob>, starting_prefix: String) {
	tokio::spawn(async move {
		// This thread will handle getting blobs and send them to the main thread for processing
		process_blob(container_client, tx, starting_prefix).await;
	});
}

async fn process_blob(container_client: Arc<ContainerClient>, tx: Sender<Blob>, starting_prefix: String) {
	// TODO: currently this is breadth first search, probably should be depth first to get to files faster
	let mut queue = VecDeque::from([starting_prefix.clone()]);
	let mut blob_count = 0u32;
	while let Some(item) = queue.pop_front() {
		if has_less_than(&item, '/', 4) {
			tracing::trace!("{item}");
		}
		// TODO: this may print multiple times for the same count since the count and iterations
		// in the loop are not directly related.
		if blob_count > 0 && blob_count % 1000 == 0 {
			tracing::trace!("{starting_prefix} -- {blob_count}");
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
				blob_count += 1;
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

	tracing::info!("{starting_prefix} -- total blob count = {blob_count} -- DONE");
}

async fn update_md5_for_blob(container_client: Arc<azure_storage_blob::clients::BlobContainerClient>, blob: Blob, chunk_size_kb: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let blob_client = container_client.blob_client(&blob.name);

	// TODO: revisit stream size
	let options = Some(azure_storage_blob::models::method_options::BlobClientDownloadOptions {
		partition_size: Some(std::num::NonZero::new(1024usize * chunk_size_kb).ok_or_else(|| "invalid chunk size")?),
		..Default::default()
	});
	let mut stream = blob_client.download(options).await?.body;
	
	let mut md5context = md5::Context::new();
	{
		while let Some(value) = stream.next().await {
			md5context.consume(value?);
		}
	}
	let md5digest = md5context.compute().0;
	tracing::info!("Computed: {:?} for {}", BASE64.encode(md5digest), blob.name);

	let prop_options = Some(azure_storage_blob::models::BlobClientSetPropertiesOptions {
		blob_content_md5: Some(md5digest.to_vec()),
		..Default::default()
	});
	let result = blob_client.set_properties(prop_options).await;

	if result.is_err() {
		tracing::error!("Failed to update md5 for {} -- {:?}", blob.name, result.err());
	}

	Ok(())
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
