use core::option::Option::Some;
use core::result::Result::Ok;
use std::sync::Arc;

use azure_storage_blob::clients::BlobContainerClient;
use azure_storage_blob::models::{BlobClientSetPropertiesOptions, BlobContainerClientListBlobsOptions, BlobItem};
use azure_storage_blob::models::method_options::BlobClientDownloadOptions;
use clap::Parser;
use futures::stream::StreamExt;
use futures::TryStreamExt;
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
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let args = Cli::parse();

	let console_filter: tracing_subscriber::EnvFilter = "trace,azure_core=warn,azure_storage=warn,hyper_util=warn,typespec_client_core=warn".into();
	let file_filter: tracing_subscriber::EnvFilter = "info,azure_core=warn,azure_storage=warn,hyper_util=warn".into();
	let console_log = tracing_subscriber::fmt::layer()
		.with_ansi(true)
		.with_writer(std::io::stderr.with_min_level(tracing::Level::WARN).or_else(std::io::stdout))
		.with_filter(console_filter);
	// TODO: I don't like the way this rolls...if the tool is running across hour lines then it creates a new file and splits it. if two instances are run then it uses the same file
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

	tracing::info!("getting container client");

	let container_client = BlobContainerClient::from_url(azure_core::Url::parse(&format!("https://{}.blob.core.windows.net/{}?{}", account, container_name, sas_token))?, None, None)?;
	let container_client = Arc::new(container_client);

	// Create a simple streaming channel
	let (tx, mut rx) = mpsc::channel(100);

	tracing::info!("Starting at {root:?}");

	start_blob_thread(container_client.clone(), tx.clone(), root.clone());

	// drop the original tx so that it doesn't hold up the rx
	drop(tx);

	// TODO: this needs testing/cleanup
	// Starts 'concurrency' (default 5) 'threads' to handle the actual calculation
	println!("started processing");
	let concurrency = args.concurrency as usize;
	let mut tasks: Vec<tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> = Vec::with_capacity(concurrency);
	for _i in 0..concurrency {
		tasks.push(tokio::spawn(async {Ok(())}));
	}

	let mut count = 0u32;
	loop {
		let mut maybe_blob = rx.recv().await;
		if maybe_blob.is_some() {
			tracing::info!("No MD5 -- {:?}", maybe_blob.as_ref().unwrap().name);
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
							let container_client = container_client.clone();
							let mut new_handle = tokio::spawn(update_md5_for_blob(container_client, blob, args.chunk_size_kb));
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

fn start_blob_thread(container_client: Arc<BlobContainerClient>, tx: Sender<BlobItem>, starting_prefix: Option<String>) {
	tokio::spawn(async move {
		// This thread will handle getting blobs and send them to the main thread for processing
		let result = process_blob(container_client, tx, starting_prefix).await;
		match result {
			Ok(()) => {},
			Err(err) => {
				tracing::error!("Error processing container, cannot continue: {err}");
			}
		}
	});
}

async fn process_blob(container_client: Arc<BlobContainerClient>, tx: Sender<BlobItem>, starting_prefix: Option<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let mut blob_count = 0u32;
	let options = match starting_prefix {
		Some(ref prefix) => Some(BlobContainerClientListBlobsOptions {
			prefix: Some(prefix.to_string()),
			..Default::default()
		}),
		None => None
	};
	let mut list_blob_resp = container_client.list_blobs(options)?;

	while let Some(blob) = list_blob_resp.try_next().await? {
		if blob_count > 0 && blob_count % 1000 == 0 {
			tracing::trace!("{starting_prefix:?} -- {blob_count}");
		}

		// Send blobs to other thread for processing
		blob_count += 1;
		if let Some(ref props) = blob.properties {
			if props.content_md5.is_none() {
				tx.send(blob.clone()).await?;
			}
		} else {
			tracing::error!("Blob with no properties? -- {:?}", blob.name);
		}
	}

	tracing::info!("{starting_prefix:?} -- total blob count = {blob_count} -- DONE");
	Ok(())
}

async fn update_md5_for_blob(container_client: Arc<BlobContainerClient>, blob: BlobItem, chunk_size_kb: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let blob_client = container_client.blob_client(blob.name.as_ref().ok_or_else(|| "Blob name is not present")?);

	// TODO: revisit stream size
	let options = Some(BlobClientDownloadOptions {
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
	tracing::info!("Computed: {:?} for {:?}", BASE64.encode(md5digest), blob.name);

	let prop_options = Some(BlobClientSetPropertiesOptions {
		blob_content_md5: Some(md5digest.to_vec()),
		..Default::default()
	});
	let result = blob_client.set_properties(prop_options).await;

	if result.is_err() {
		tracing::error!("Failed to update md5 for {:?} -- {:?}", blob.name, result.err());
	}

	Ok(())
}
