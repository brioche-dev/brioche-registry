use std::{
    borrow::Cow, collections::HashSet, net::SocketAddr, str::FromStr as _, sync::Arc,
    time::Duration,
};

use argon2::PasswordVerifier;
use axum::body::Body;
use axum_extra::headers::{authorization::Basic, Authorization};
use brioche::{
    artifact::{ArtifactHash, CompleteArtifact, LazyArtifact},
    blob::BlobId,
    project::{Project, ProjectHash, ProjectListing},
    registry::{
        CreateResolveRequest, CreateResolveResponse, GetProjectTagResponse, GetResolveResponse,
        PublishProjectResponse, UpdatedTag,
    },
};
use bstr::ByteSlice as _;
use eyre::{Context as _, OptionExt as _};
use futures::{StreamExt as _, TryStreamExt as _};
use joinery::JoinableIterator as _;
use sqlx::Arguments as _;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};
use tracing::Span;

pub async fn start_server(state: Arc<ServerState>, addr: &SocketAddr) -> eyre::Result<()> {
    let proxy_layers = state.proxy_layers;
    let trace_layer = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(move |req: &axum::http::Request<Body>| {
            let request_id = ulid::Ulid::new();

            let connect_info = req
                .extensions()
                .get::<axum::extract::ConnectInfo<SocketAddr>>();
            let received_ip = connect_info.map(|connect_info| connect_info.0.ip().to_string());
            let received_ip = received_ip.as_deref().unwrap_or("<unknown>");
            let forwarded_for = req
                .headers()
                .get_all("X-Forwarded-For")
                .into_iter()
                .flat_map(|forwarded_for| forwarded_for.as_bytes().split_str(","))
                .map(|forwarded_for| String::from_utf8_lossy(forwarded_for.trim()));
            let client_ips = [Cow::Borrowed(received_ip)].into_iter().chain(forwarded_for);
            let client_ip = client_ips
                .take(proxy_layers + 1)
                .last()
                .unwrap_or(Cow::Borrowed("<unknown>"));

            tracing::info_span!("request", method = %req.method(), path = %req.uri().path(), %client_ip, %request_id)
        })
        .on_request(|_req: &axum::http::Request<Body>, _span: &Span| {
            tracing::info!("received request")
        })
        .on_response(
            |res: &axum::http::Response<Body>, latency: Duration, _span: &Span| {
                tracing::info!(
                    status = res.status().as_u16(),
                    latency = latency.as_secs_f32(),
                    "response"
                )
            },
        )
        .on_failure(
            |err: tower_http::classify::ServerErrorsFailureClass,
             latency: Duration,
             _span: &Span| {
                tracing::error!(error = %err, latency = latency.as_secs_f32(), "request failed")
            },
        );

    let app = axum::Router::new()
        .route("/v0/healthcheck", axum::routing::get(healthcheck_handler))
        .route("/v0/projects", axum::routing::post(publish_project_handler))
        .route(
            "/v0/projects/:project_hash",
            axum::routing::get(get_project_handler),
        )
        .route(
            "/v0/project-tags/:project_name/:tag",
            axum::routing::get(get_project_tag_handler),
        )
        .route(
            "/v0/blobs/:blob_id",
            axum::routing::get(get_blob_handler)
                .head(head_blob_handler)
                .put(put_blob_handler),
        )
        .route(
            "/v0/artifacts/:artifact_hash",
            axum::routing::get(get_artifact_handler).put(put_artifact_handler),
        )
        .route(
            "/v0/artifacts/:artifact_hash/resolve",
            axum::routing::get(get_resolve_handler).post(create_resolve_handler),
        )
        .route(
            "/v0/known-artifacts",
            axum::routing::post(known_artifacts_handler),
        )
        .route("/v0/known-blobs", axum::routing::post(known_blobs_handler))
        .route(
            "/v0/known-resolves",
            axum::routing::post(known_resolves_handler),
        )
        .layer(trace_layer)
        .with_state(state.clone());
    let app = app.into_make_service_with_connect_info::<SocketAddr>();

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let listen_addr = listener.local_addr()?;
    tracing::info!("listening on {listen_addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("shutting down");
    state.handle_shutdown().await;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c()
        .await
        .inspect_err(|error| tracing::error!(%error, "failed while awaiting shutdown signal"));
}

pub struct ServerState {
    env: super::ServerEnv,
    proxy_layers: usize,
    object_store: Box<dyn object_store::ObjectStore>,
    object_store_path: object_store::path::Path,
    pub db_pool: sqlx::SqlitePool,
}

impl ServerState {
    pub async fn new(env: super::ServerEnv) -> eyre::Result<Self> {
        // Handle the special `relative-file` URL (primarily for development)
        let object_store_url = match env.object_store_url.scheme() {
            "relative-file" => {
                let relative_path = env.object_store_url.path();
                let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                let abs_path = tokio::fs::canonicalize(std::path::Path::new(relative_path))
                    .await
                    .with_context(|| {
                        format!(
                            "failed to canonicalize relative object store path: {relative_path}"
                        )
                    })?;
                url::Url::from_directory_path(abs_path)
                    .map_err(|_| eyre::eyre!("failed to create object store URL"))?
            }
            _ => env.object_store_url.clone(),
        };
        let object_store_opts = std::env::vars().map(|(k, v)| (k.to_ascii_lowercase(), v));
        let (object_store, object_store_path) =
            object_store::parse_url_opts(&object_store_url, object_store_opts)?;

        let db_opts = sqlx::sqlite::SqliteConnectOptions::from_str(&env.database_url)?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let db_filename = db_opts.clone().get_filename();
        let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .connect_with(db_opts)
            .await?;

        let proxy_layers = env.proxy_layers.unwrap_or(0);

        tracing::info!(db_filename = %db_filename.display(), "set up database connection");

        Ok(Self {
            env,
            proxy_layers,
            object_store,
            object_store_path,
            db_pool,
        })
    }

    fn password_hash(&self) -> eyre::Result<argon2::PasswordHash<'_>> {
        let password_hash =
            argon2::PasswordHash::new(&self.env.password_hash).wrap_err("invalid password hash")?;
        Ok(password_hash)
    }

    async fn handle_shutdown(&self) {
        self.db_pool.close().await;
    }
}

async fn healthcheck_handler() -> axum::Json<Healthcheck> {
    axum::Json(Healthcheck { status: "ok" })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Healthcheck {
    status: &'static str,
}

async fn get_project_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(project_hash): axum::extract::Path<ProjectHash>,
) -> Result<axum::Json<Project>, ServerError> {
    let project_hash_value = project_hash.to_string();
    let record = sqlx::query!(
        "SELECT project_json FROM projects WHERE project_hash = ?",
        project_hash_value,
    )
    .fetch_all(&state.db_pool)
    .await
    .wrap_err("failed to fetch project from database")?;
    let record = record.first().ok_or(ServerError::NotFound)?;

    let project: Project =
        serde_json::from_str(&record.project_json).wrap_err("failed to parse project")?;
    Ok(axum::Json(project))
}

async fn get_project_tag_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path((project_name, tag)): axum::extract::Path<(String, String)>,
) -> Result<axum::Json<GetProjectTagResponse>, ServerError> {
    let records = sqlx::query!(
        "SELECT project_hash FROM project_tags WHERE name = ? AND tag = ? AND is_current",
        project_name,
        tag,
    )
    .fetch_all(&state.db_pool)
    .await
    .wrap_err("failed to fetch project tags from database")?;
    let record = records.first().ok_or(ServerError::NotFound)?;

    let project_hash: Result<ProjectHash, _> = record.project_hash.parse();
    let project_hash = project_hash
        .map_err(|error| eyre::eyre!(error))
        .wrap_err("failed to parse project hash from database")
        .map_err(ServerError::other)?;
    let response = GetProjectTagResponse { project_hash };
    Ok(axum::Json(response))
}

async fn publish_project_handler(
    Authenticated(state): Authenticated,
    axum::Json(project_listing): axum::Json<ProjectListing>,
) -> Result<(axum::http::StatusCode, axum::Json<PublishProjectResponse>), ServerError> {
    let mut new_files = 0;
    for (file_id, file_contents) in &project_listing.files {
        let file_path = state
            .object_store_path
            .child("blobs")
            .child(file_id.to_string());
        let head_result = state.object_store.head(&file_path).await;

        match head_result {
            Ok(_) => {
                // File already uploaded, so ignore
                continue;
            }
            Err(object_store::Error::NotFound { .. }) => {
                // File does not exist, so upload
            }
            Err(error) => {
                return Err(ServerError::other(
                    eyre::Error::new(error)
                        .wrap_err("failed to check if project file already exists"),
                ));
            }
        }

        state
            .object_store
            .put(&file_path, bytes::Bytes::copy_from_slice(file_contents))
            .await
            .wrap_err("failed to upload new project file")
            .map_err(ServerError::other)?;

        new_files += 1;
    }

    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut new_projects = 0;
    for (project_hash, project) in &project_listing.projects {
        let project_hash_value = project_hash.to_string();
        let project_json = serde_json::to_string(&project).map_err(ServerError::other)?;
        let result = sqlx::query!(
            "INSERT OR IGNORE INTO projects (project_hash, project_json) VALUES (?, ?)",
            project_hash_value,
            project_json
        )
        .execute(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        new_projects += result.rows_affected();
    }

    let root_project_hash = project_listing.root_project;
    let root_project_hash_value = root_project_hash.to_string();
    let root_project = project_listing
        .projects
        .get(&project_listing.root_project)
        .ok_or(ServerError::BadRequest(Cow::Borrowed(
            "root project not found in project list",
        )))?;
    let root_project_name =
        root_project
            .definition
            .name
            .as_ref()
            .ok_or(ServerError::BadRequest(Cow::Borrowed(
                "cannot publish a project without a name",
            )))?;

    // Tag with `latest` plus the version number
    let root_project_tags = ["latest"]
        .into_iter()
        .chain(root_project.definition.version.as_deref());

    let mut tags = vec![];

    for tag in root_project_tags {
        let update_result = sqlx::query!(
            r#"
                UPDATE project_tags
                SET is_current = NULL
                WHERE
                    name = ?
                    AND tag = ?
                    AND project_hash <> ?
                    AND is_current
                RETURNING project_hash
            "#,
            root_project_name,
            tag,
            root_project_hash_value,
        )
        .fetch_all(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        let inserted_records = sqlx::query!(
            r#"
                INSERT INTO project_tags (
                    name,
                    tag,
                    project_hash,
                    is_current
                ) VALUES (?, ?, ?, TRUE)
                ON CONFLICT (name, tag, is_current) DO NOTHING
                RETURNING name, tag
            "#,
            root_project_name,
            tag,
            root_project_hash_value,
        )
        .fetch_all(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        // Run a query to make sure the current tag matches the expected
        // project hash. This is a sanity check

        let records = sqlx::query!(
            r#"
                SELECT project_hash
                FROM project_tags
                WHERE name = ? AND tag = ? AND is_current = TRUE
            "#,
            root_project_name,
            tag
        )
        .fetch_all(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        let record = records
            .first()
            .ok_or_eyre("failed to get updated project tag")
            .map_err(ServerError::other)?;
        let record_project_hash: Result<ProjectHash, _> = record.project_hash.parse();
        let record_project_hash = record_project_hash
            .map_err(|error| eyre::eyre!(error))
            .map_err(ServerError::other)?;
        if record_project_hash != root_project_hash {
            return Err(ServerError::Other(eyre::eyre!("project tag did not match")));
        }

        let previous_hash = update_result
            .first()
            .and_then(|record| record.project_hash.parse().ok());

        for record in inserted_records {
            tags.push(UpdatedTag {
                name: record.name.to_string(),
                tag: record.tag.to_string(),
                previous_hash,
            });
        }
    }

    db_transaction.commit().await.map_err(ServerError::other)?;

    tracing::info!(new_files, new_projects, root_project = %project_listing.root_project, ?tags, "published project");

    let response = PublishProjectResponse {
        root_project: project_listing.root_project,
        new_files,
        new_projects,
        tags,
    };
    Ok((axum::http::StatusCode::CREATED, axum::Json(response)))
}

async fn head_blob_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(blob_id): axum::extract::Path<BlobId>,
) -> Result<axum::response::Response, ServerError> {
    let blob_path = state
        .object_store_path
        .child("blobs")
        .child(blob_id.to_string());
    let object = state.object_store.head(&blob_path).await;
    let body = match object {
        Ok(_) => {
            // Build an empty body from a stream. Unlike `Body::empty()`, this
            // ensures that the `Content-Length` header is not set.
            const EMPTY: [Result<axum::body::Bytes, ServerError>; 0] = [];
            Body::from_stream(futures::stream::iter(EMPTY))
        }
        Err(object_store::Error::NotFound { .. }) => {
            return Err(ServerError::NotFound);
        }
        Err(error) => {
            return Err(ServerError::other(error));
        }
    };

    let response = axum::response::Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .header(axum::http::header::TRANSFER_ENCODING, "chunked")
        .body(body)
        .map_err(ServerError::other)?;
    Ok(response)
}

async fn get_blob_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(blob_id): axum::extract::Path<BlobId>,
) -> Result<axum::response::Response, ServerError> {
    let blob_path = state
        .object_store_path
        .child("blobs")
        .child(blob_id.to_string());
    let object = state.object_store.get(&blob_path).await;
    let object = match object {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(ServerError::NotFound);
        }
        Err(error) => {
            return Err(ServerError::other(error));
        }
    };

    let body = match object.payload {
        object_store::GetResultPayload::File(file, _) => {
            let file = tokio::fs::File::from_std(file);
            let stream = tokio_util::io::ReaderStream::new(file);
            axum::body::Body::from_stream(stream)
        }
        object_store::GetResultPayload::Stream(stream) => axum::body::Body::from_stream(stream),
    };
    let response = axum::response::Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .map_err(ServerError::other)?;
    Ok(response)
}

async fn put_blob_handler(
    Authenticated(state): Authenticated,
    axum::extract::Path(blob_id): axum::extract::Path<BlobId>,
    body: axum::body::Body,
) -> Result<(axum::http::StatusCode, axum::Json<BlobId>), ServerError> {
    let blob_path = state
        .object_store_path
        .child("blobs")
        .child(blob_id.to_string());

    // Return an error if the blob doesn't exist

    let head = state.object_store.head(&blob_path).await;
    match head {
        Ok(_) => {
            return Err(ServerError::AlreadyExists);
        }
        Err(object_store::Error::NotFound { .. }) => {
            // Object doesn't exist, so we can create it
        }
        Err(error) => {
            return Err(ServerError::other(error));
        }
    }

    // Start a multipart upload to the object store. We do this so we can
    // hash the blob while we upload it, then cancel the upload if we discover
    // the hash doesn't match.
    // NOTE: Past this point, we shouldn't return early before calling
    // `abort_multipart` for the object (to abort the upload) or calling
    // `finalize` on the writer (to finish the upload).

    let (multipart_id, mut object_writer) = state
        .object_store
        .put_multipart(&blob_path)
        .await
        .map_err(ServerError::other)?;

    let (hasher_tx, mut hasher_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(10);
    let (uploader_tx, uploader_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(10);

    // Create a task that hashes each chunk of the blob as we receive it
    let hasher_task = tokio::task::spawn_blocking(move || {
        let mut hasher = blake3::Hasher::new();
        while let Some(bytes) = hasher_rx.blocking_recv() {
            hasher.update(&bytes[..]);
        }

        hasher.finalize()
    });

    // Create a task to upload each chunk of the blob
    let uploader_task = tokio::spawn(async move {
        // Create a buffered reader from the bytes from the receiver. We use
        // a size of 1MB because each buffered chunk should get uploaded as a
        // separate `PUT` request.
        let uploader_stream =
            tokio_stream::wrappers::ReceiverStream::new(uploader_rx).map(Ok::<_, std::io::Error>);
        let uploader_reader = tokio_util::io::StreamReader::new(uploader_stream);
        let mut uploader_reader = tokio::io::BufReader::with_capacity(1024 * 1024, uploader_reader);

        loop {
            // Read a chunk from the receiver, up to the buffer size
            let chunk = uploader_reader.fill_buf().await?;
            let chunk_len = chunk.len();

            // A length of 0 means we've reached the end
            if chunk_len == 0 {
                break;
            }

            // Write the buffered chunk
            object_writer
                .write_all(chunk)
                .await
                .wrap_err("failed to write blob chunk")?;

            // Consume the bytes we just read from the buffered reader
            uploader_reader.consume(chunk_len);
        }

        eyre::Ok(object_writer)
    });

    let mut body_stream = body.into_data_stream();
    let result = loop {
        let Some(bytes) = body_stream.next().await else {
            break Ok(());
        };

        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(error) => {
                break Err(ServerError::other(error));
            }
        };

        // Send the bytes to both the hasher and the uploader tasks

        match hasher_tx.send(bytes.clone()).await {
            Ok(()) => {}
            Err(error) => {
                break Err(ServerError::other(error));
            }
        };
        match uploader_tx.send(bytes).await {
            Ok(()) => {}
            Err(error) => {
                break Err(ServerError::other(error));
            }
        }
    };

    // Close the channels by dropping them
    drop(hasher_tx);
    drop(uploader_tx);

    // Abort the multipart upload if we encountered an error reading from
    // the body
    if let Err(error) = result {
        let _ = state
            .object_store
            .abort_multipart(&blob_path, &multipart_id)
            .await
            .inspect_err(|error| {
                tracing::warn!(?blob_path, %error, "failed to abort multipart upload");
            });
        return Err(error);
    }

    // Wait for the hasher to finish (aborting the mutlipart upload if it failed)
    let actual_hash = match hasher_task.await {
        Ok(hash) => hash,
        Err(error) => {
            let _ = state
                .object_store
                .abort_multipart(&blob_path, &multipart_id)
                .await
                .inspect_err(|error| {
                    tracing::warn!(?blob_path, %error, "failed to abort multipart upload");
                });
            return Err(ServerError::other(error));
        }
    };

    // Wait for the uploader task to finish (aborting the multipart upload
    // if it fails). If it succeeds, we get the object writer back so we can
    // flush it.
    let uploader_task_result = uploader_task
        .await
        .map_err(eyre::Error::from)
        .and_then(|result| result);
    let mut object_writer = match uploader_task_result {
        Ok(object_writer) => object_writer,
        Err(error) => {
            let _ = state
                .object_store
                .abort_multipart(&blob_path, &multipart_id)
                .await
                .inspect_err(|error| {
                    tracing::warn!(?blob_path, %error, "failed to abort multipart upload");
                });
            return Err(ServerError::other(error));
        }
    };

    // Validate the hash of the blob matches the request
    let actual_blob_id = BlobId::from_blake3(actual_hash);
    if blob_id != actual_blob_id {
        state
            .object_store
            .abort_multipart(&blob_path, &multipart_id)
            .await
            .inspect_err(|error| {
                tracing::warn!(?blob_path, %error, "failed to abort multipart upload");
            })
            .map_err(ServerError::other)?;
        return Err(ServerError::BadRequest(Cow::Owned(format!(
            "blob hash mismatch: expected {blob_id}, got {actual_blob_id}"
        ))));
    }

    // Flush the stream and shutdown the writer to ensure we finish the
    // multipart upload
    object_writer
        .flush()
        .await
        .wrap_err("failed to flush blob writer")
        .map_err(ServerError::other)?;
    object_writer
        .shutdown()
        .await
        .wrap_err("failed to shutdown blob writer")?;

    Ok((axum::http::StatusCode::CREATED, axum::Json(blob_id)))
}

async fn get_artifact_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(artifact_hash): axum::extract::Path<ArtifactHash>,
) -> Result<axum::Json<LazyArtifact>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let artifact_hash_value = artifact_hash.to_string();
    let record = sqlx::query!(
        "SELECT artifact_json FROM artifacts WHERE artifact_hash = ?",
        artifact_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    match record {
        Some(record) => {
            let artifact: LazyArtifact = serde_json::from_str(&record.artifact_json)
                .wrap_err_with(|| {
                    format!("failed to deserialize artifact JSON with hash {artifact_hash}")
                })
                .map_err(ServerError::other)?;
            Ok(axum::Json(artifact))
        }
        None => Err(ServerError::NotFound),
    }
}

async fn put_artifact_handler(
    Authenticated(state): Authenticated,
    axum::extract::Path(artifact_hash): axum::extract::Path<ArtifactHash>,
    axum::Json(artifact): axum::Json<LazyArtifact>,
) -> Result<(axum::http::StatusCode, axum::Json<ArtifactHash>), ServerError> {
    if artifact_hash != artifact.hash() {
        return Err(ServerError::BadRequest(Cow::Owned(format!(
            "expected artifact hash to be {artifact_hash}, but was {}",
            artifact.hash()
        ))));
    }

    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let artifact_hash_value = artifact_hash.to_string();
    let artifact_json_value = serde_json::to_string(&artifact).map_err(ServerError::other)?;
    let result = sqlx::query!(
        r#"
            INSERT INTO artifacts (artifact_hash, artifact_json)
            VALUES (?, ?)
            ON CONFLICT (artifact_hash) DO NOTHING
        "#,
        artifact_hash_value,
        artifact_json_value,
    )
    .execute(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    if result.rows_affected() == 0 {
        Ok((axum::http::StatusCode::OK, axum::Json(artifact_hash)))
    } else {
        Ok((axum::http::StatusCode::CREATED, axum::Json(artifact_hash)))
    }
}

async fn known_artifacts_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::Json(artifact_hashes): axum::Json<HashSet<ArtifactHash>>,
) -> Result<axum::Json<HashSet<ArtifactHash>>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut arguments = sqlx::sqlite::SqliteArguments::default();
    for hash in &artifact_hashes {
        arguments.add(hash.to_string());
    }

    let placeholders = std::iter::repeat("?")
        .take(artifact_hashes.len())
        .join_with(", ");

    let rows = sqlx::query_as_with::<_, (String,), _>(
        &format!(
            r#"
            SELECT artifact_hash
            FROM artifacts
            WHERE artifact_hash IN ({placeholders})
        "#,
        ),
        arguments,
    )
    .fetch_all(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let results = rows
        .into_iter()
        .map(|row| row.0.parse())
        .collect::<Result<HashSet<ArtifactHash>, _>>()
        .map_err(|error| eyre::eyre!(error))
        .map_err(ServerError::other)?;
    Ok(axum::Json(results))
}

async fn known_blobs_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::Json(blob_ids): axum::Json<HashSet<BlobId>>,
) -> Result<axum::Json<HashSet<BlobId>>, ServerError> {
    let (tx, rx) = tokio::sync::mpsc::channel(100);

    let collect_task = tokio::task::spawn(async move {
        tokio_stream::wrappers::ReceiverStream::new(rx)
            .collect::<HashSet<BlobId>>()
            .await
    });

    futures::stream::iter(blob_ids)
        .map(Ok)
        .try_for_each_concurrent(Some(100), |blob_id| {
            let state = state.clone();
            let tx = tx.clone();
            async move {
                let blob_path = state
                    .object_store_path
                    .child("blobs")
                    .child(blob_id.to_string());
                let head = state.object_store.head(&blob_path).await;
                match head {
                    Ok(_) => {
                        tx.send(blob_id).await.map_err(ServerError::other)?;
                        Ok(())
                    }
                    Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(error) => Err(ServerError::other(error)),
                }
            }
        })
        .await?;

    // Drop the sender so the receiving side can finish
    drop(tx);

    let result = collect_task.await.map_err(ServerError::other)?;
    Ok(axum::Json(result))
}

async fn known_resolves_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::Json(artifact_hashes): axum::Json<HashSet<(ArtifactHash, ArtifactHash)>>,
) -> Result<axum::Json<HashSet<(ArtifactHash, ArtifactHash)>>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut arguments = sqlx::sqlite::SqliteArguments::default();
    for (input_hash, output_hash) in &artifact_hashes {
        arguments.add(input_hash.to_string());
        arguments.add(output_hash.to_string());
    }

    let placeholders = std::iter::repeat("(?, ?)")
        .take(artifact_hashes.len())
        .join_with(", ");

    let rows = sqlx::query_as_with::<_, (String, String), _>(
        &format!(
            r#"
            SELECT input_hash, output_hash
            FROM resolves
            WHERE (input_hash, output_hash ) IN (VALUES {placeholders})
        "#,
        ),
        arguments,
    )
    .fetch_all(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let results = rows
        .into_iter()
        .map(|row| {
            let input_hash: Result<ArtifactHash, _> = row.0.parse();
            let input_hash = input_hash.map_err(|error| eyre::eyre!(error))?;
            let output_hash: Result<ArtifactHash, _> = row.1.parse();
            let output_hash = output_hash.map_err(|error| eyre::eyre!(error))?;
            eyre::Ok((input_hash, output_hash))
        })
        .collect::<Result<HashSet<(ArtifactHash, ArtifactHash)>, _>>()
        .map_err(ServerError::other)?;
    Ok(axum::Json(results))
}

async fn get_resolve_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(artifact_hash): axum::extract::Path<ArtifactHash>,
) -> Result<axum::Json<GetResolveResponse>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let artifact_hash_value = artifact_hash.to_string();
    let record = sqlx::query!(
        r#"
            SELECT artifact_hash, artifact_json
            FROM artifacts
            INNER JOIN resolves
                ON resolves.output_hash = artifacts.artifact_hash
            WHERE resolves.input_hash = ? AND resolves.is_canonical
        "#,
        artifact_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    let Some(record) = record else {
        return Err(ServerError::NotFound);
    };

    let output_artifact: CompleteArtifact = serde_json::from_str(&record.artifact_json)
        .wrap_err_with(|| format!("failed to deserialize artifact JSON with hash {artifact_hash}"))
        .map_err(ServerError::other)?;
    let record_artifact_hash: Result<ArtifactHash, _> = record.artifact_hash.parse();
    let record_artifact_hash = record_artifact_hash
        .map_err(|error| eyre::eyre!(error))
        .wrap_err_with(|| format!("failed to parse artifact hash {}", record.artifact_hash))
        .map_err(ServerError::other)?;

    if output_artifact.hash() != record_artifact_hash {
        return Err(ServerError::Other(eyre::eyre!(
            "artifact hash {} did not match expected hash {record_artifact_hash}",
            output_artifact.hash(),
        )));
    }

    let response = GetResolveResponse {
        output_hash: output_artifact.hash(),
        output_artifact,
    };
    Ok(axum::Json(response))
}

async fn create_resolve_handler(
    Authenticated(state): Authenticated,
    axum::extract::Path(input_hash): axum::extract::Path<ArtifactHash>,
    axum::Json(request): axum::Json<CreateResolveRequest>,
) -> Result<(axum::http::StatusCode, axum::Json<CreateResolveResponse>), ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let input_hash_value = input_hash.to_string();
    let input_result = sqlx::query!(
        r#"
            SELECT artifact_json
            FROM artifacts
            WHERE artifact_hash = ?
        "#,
        input_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let Some(input_result) = input_result else {
        return Err(ServerError::NotFound);
    };

    let input_artifact: LazyArtifact = serde_json::from_str(&input_result.artifact_json)
        .wrap_err_with(|| {
            format!("failed to deserialize input artifact JSON with hash {input_hash}")
        })
        .map_err(ServerError::other)?;
    if input_artifact.hash() != input_hash {
        return Err(ServerError::Other(eyre::eyre!(
            "artifact hash {} did not match expected hash {input_hash}",
            input_artifact.hash()
        )));
    }

    let output_hash = request.output_hash;
    let output_hash_value = output_hash.to_string();
    let output_result = sqlx::query!(
        r#"
            SELECT artifact_json
            FROM artifacts
            WHERE artifact_hash = ?
        "#,
        output_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let Some(output_result) = output_result else {
        return Err(ServerError::BadRequest(Cow::Owned(format!(
            "output artifact {output_hash} does not exist"
        ))));
    };

    let output_artifact: LazyArtifact = serde_json::from_str(&output_result.artifact_json)
        .wrap_err_with(|| {
            format!("failed to deserialize output artifact JSON with hash {output_hash}")
        })
        .map_err(ServerError::other)?;
    let _output_artifact: CompleteArtifact = output_artifact.try_into().map_err(|_| {
        ServerError::BadRequest(Cow::Owned(format!(
            "output artifact {output_hash} is not a complete artifact"
        )))
    })?;

    let canonical_result = sqlx::query!(
        r#"
            SELECT output_hash
            FROM resolves
            WHERE input_hash = ? AND is_canonical
        "#,
        input_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let (canonical_output_hash, is_new) = match canonical_result {
        Some(canonical_result) => {
            let canonical_output_hash: Result<ArtifactHash, _> =
                canonical_result.output_hash.parse();
            let canonical_output_hash = canonical_output_hash
                .map_err(|error| eyre::eyre!(error))
                .map_err(ServerError::other)?;

            let insert_result = sqlx::query!(
                r#"
                    INSERT INTO resolves (input_hash, output_hash, is_canonical)
                    VALUES (?, ?, FALSE)
                    ON CONFLICT (input_hash, output_hash) DO NOTHING
                "#,
                input_hash_value,
                output_hash_value,
            )
            .execute(&mut *db_transaction)
            .await
            .map_err(ServerError::other)?;

            if insert_result.rows_affected() > 0 {
                tracing::info!(%input_hash, %output_hash, "added resolve (non-canonical)");
                (canonical_output_hash, true)
            } else {
                tracing::info!(%input_hash, %output_hash, "received resolve, but already exists (non-canonical)");
                (canonical_output_hash, false)
            }
        }
        None => {
            sqlx::query!(
                r#"
                    INSERT INTO resolves (input_hash, output_hash, is_canonical)
                    VALUES (?, ?, TRUE)
                "#,
                input_hash_value,
                output_hash_value,
            )
            .execute(&mut *db_transaction)
            .await
            .map_err(ServerError::other)?;

            tracing::info!(%input_hash, %output_hash, "added resolve (canonical)");

            (output_hash, true)
        }
    };

    db_transaction.commit().await.map_err(ServerError::other)?;

    let status = if is_new {
        axum::http::StatusCode::CREATED
    } else {
        axum::http::StatusCode::OK
    };
    let response = CreateResolveResponse {
        canonical_output_hash,
    };

    Ok((status, axum::Json(response)))
}

struct Authenticated(Arc<ServerState>);

#[async_trait::async_trait]
impl axum::extract::FromRequestParts<Arc<ServerState>> for Authenticated {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let axum_extra::TypedHeader(authorization) =
            axum_extra::TypedHeader::<Authorization<Basic>>::from_request_parts(parts, &()).await?;

        let username_matches = authorization.username() == "admin";
        let password_matches = argon2::Argon2::default()
            .verify_password(authorization.password().as_bytes(), &state.password_hash()?)
            .is_ok();
        if username_matches && password_matches {
            Ok(Self(state.clone()))
        } else {
            Err(ServerError::InvalidCredentials)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ServerError {
    #[error("internal error")]
    Other(#[from] eyre::Error),

    #[error("not found")]
    NotFound,

    #[error("{0}")]
    BadRequest(Cow<'static, str>),

    #[error("username or password did not match")]
    InvalidCredentials,

    #[error("resource already exists")]
    AlreadyExists,

    #[error(transparent)]
    TypedHeaderRejection(#[from] axum_extra::typed_header::TypedHeaderRejection),
}

impl ServerError {
    fn other(error: impl Into<eyre::Error>) -> Self {
        Self::Other(error.into())
    }
}

impl axum::response::IntoResponse for ServerError {
    fn into_response(self) -> axum::response::Response {
        let body = serde_json::json!({
            "error": self.to_string(),
        });

        let status_code = match self {
            ServerError::Other(error) => {
                tracing::error!("internal error: {error:#}");
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
            ServerError::BadRequest(_) => axum::http::StatusCode::BAD_REQUEST,
            ServerError::AlreadyExists => axum::http::StatusCode::CONFLICT,
            ServerError::NotFound => axum::http::StatusCode::NOT_FOUND,
            ServerError::InvalidCredentials => axum::http::StatusCode::UNAUTHORIZED,
            ServerError::TypedHeaderRejection(rejection) => rejection.into_response().status(),
        };

        (status_code, axum::response::Json(body)).into_response()
    }
}
