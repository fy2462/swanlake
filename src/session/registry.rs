//! Session registry - manages all active client sessions.
//!
//! The registry:
//! - Creates new sessions on client connect
//! - Tracks active sessions by ID
//! - Provides session lookup
//! - Cleans up idle sessions
//! - Enforces max session limit

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tracing::{debug, info, instrument, warn};

use crate::config::ServerConfig;
use crate::engine::EngineFactory;
use crate::error::ServerError;
use crate::session::id::SessionId;
use crate::session::session::Session;

/// Registry for managing all active sessions
#[derive(Clone)]
pub struct SessionRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    factory: EngineFactory,
    max_sessions: usize,
    session_timeout: Duration,
    writes_enabled: bool,
}

struct RegistryInner {
    sessions: HashMap<SessionId, Arc<Session>>,
}

impl SessionRegistry {
    /// Create a new session registry
    #[instrument(skip(config))]
    pub fn new(config: &ServerConfig) -> Result<Self, ServerError> {
        let factory = EngineFactory::new(config)?;
        let max_sessions = config.max_sessions.unwrap_or(100);
        let session_timeout = Duration::from_secs(config.session_timeout_seconds.unwrap_or(1800)); // 30min default

        info!(
            max_sessions,
            session_timeout_seconds = session_timeout.as_secs(),
            "session registry initialized"
        );

        Ok(Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                sessions: HashMap::new(),
            })),
            factory,
            max_sessions,
            session_timeout,
            writes_enabled: config.enable_writes,
        })
    }

    /// Create a new session with a dedicated connection
    #[instrument(skip(self))]
    pub fn create_session(&self) -> Result<Arc<Session>, ServerError> {
        // Check session limit
        {
            let inner = self.inner.read().expect("registry lock poisoned");
            if inner.sessions.len() >= self.max_sessions {
                warn!(
                    current = inner.sessions.len(),
                    max = self.max_sessions,
                    "max sessions limit reached"
                );
                return Err(ServerError::MaxSessionsReached);
            }
        }

        // Create new connection
        let connection = self.factory.create_connection()?;

        // Start UI server if needed (only once per factory)
        if self.factory.enable_ui_server() {
            // UI server is started by first connection initialization
            // This is handled in EngineFactory if needed
        }

        // Create session
        let session = Arc::new(Session::new(connection, self.writes_enabled));
        let session_id = session.id().clone();

        // Register session
        {
            let mut inner = self.inner.write().expect("registry lock poisoned");
            inner.sessions.insert(session_id.clone(), session.clone());
            info!(
                session_id = %session_id,
                total_sessions = inner.sessions.len(),
                "session created"
            );
        }

        Ok(session)
    }

    /// Get an existing session by ID
    #[instrument(skip(self), fields(session_id = %session_id))]
    pub fn get_session(&self, session_id: &SessionId) -> Result<Arc<Session>, ServerError> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner
            .sessions
            .get(session_id)
            .cloned()
            .ok_or(ServerError::SessionNotFound)
    }

    /// Get an existing session by ID, or create a new one if not found
    #[instrument(skip(self))]
    pub fn get_or_create_session(
        &self,
        session_id: Option<&SessionId>,
    ) -> Result<Arc<Session>, ServerError> {
        if let Some(id) = session_id {
            // Try to get existing session
            match self.get_session(id) {
                Ok(session) => {
                    debug!(session_id = %id, "reusing existing session");
                    return Ok(session);
                }
                Err(ServerError::SessionNotFound) => {
                    debug!(session_id = %id, "session not found, creating new one");
                }
                Err(e) => return Err(e),
            }
        }

        // Create new session
        self.create_session()
    }

    /// Get an existing session by ID, or create a new one if not found
    /// Returns (session, was_created) where was_created is true if a new session was created
    #[instrument(skip(self))]
    pub fn get_or_create_session_with_info(
        &self,
        session_id: Option<&SessionId>,
    ) -> Result<(Arc<Session>, bool), ServerError> {
        if let Some(id) = session_id {
            // Try to get existing session
            match self.get_session(id) {
                Ok(session) => {
                    debug!(session_id = %id, "reusing existing session");
                    return Ok((session, false));
                }
                Err(ServerError::SessionNotFound) => {
                    debug!(session_id = %id, "session not found, creating new one");
                }
                Err(e) => return Err(e),
            }
        }

        // Create new session
        let session = self.create_session()?;
        Ok((session, true))
    }

    /// Remove a session by ID
    #[instrument(skip(self), fields(session_id = %session_id))]
    pub fn remove_session(&self, session_id: &SessionId) -> Result<(), ServerError> {
        let mut inner = self.inner.write().expect("registry lock poisoned");
        if inner.sessions.remove(session_id).is_some() {
            info!(
                session_id = %session_id,
                total_sessions = inner.sessions.len(),
                "session removed"
            );
            Ok(())
        } else {
            Err(ServerError::SessionNotFound)
        }
    }

    /// Clean up idle sessions that have exceeded the timeout
    #[instrument(skip(self))]
    pub fn cleanup_idle_sessions(&self) -> usize {
        let mut inner = self.inner.write().expect("registry lock poisoned");
        let before = inner.sessions.len();

        inner.sessions.retain(|id, session| {
            if session.idle_duration() > self.session_timeout {
                info!(
                    session_id = %id,
                    idle_duration = ?session.idle_duration(),
                    "removing idle session"
                );
                false
            } else {
                true
            }
        });

        let removed = before - inner.sessions.len();
        if removed > 0 {
            info!(
                removed,
                total_sessions = inner.sessions.len(),
                "cleaned up idle sessions"
            );
        }
        removed
    }

    /// Get current number of active sessions
    pub fn session_count(&self) -> usize {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner.sessions.len()
    }

    /// Get max sessions limit
    pub fn max_sessions(&self) -> usize {
        self.max_sessions
    }

    /// Get session timeout duration
    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    /// Check if writes are enabled
    pub fn writes_enabled(&self) -> bool {
        self.writes_enabled
    }

    /// Get or create session by session ID (Phase 2)
    ///
    /// This enables session persistence across requests from the same gRPC connection.
    /// The session_id is derived from the connection info (e.g., remote address).
    /// If a session already exists with this ID, it is reused.
    /// Otherwise, a new session is created with the given ID.
    #[instrument(skip(self))]
    pub fn get_or_create_by_id(&self, session_id: &SessionId) -> Result<Arc<Session>, ServerError> {
        // First, try to get existing session (read lock)
        {
            let inner = self.inner.read().expect("registry lock poisoned");
            if let Some(session) = inner.sessions.get(session_id) {
                debug!(
                    session_id = %session_id,
                    "reusing existing session"
                );
                return Ok(session.clone());
            }
        }

        // No existing session, create new one with specific ID (write lock)
        // Check session limit
        {
            let inner = self.inner.read().expect("registry lock poisoned");
            if inner.sessions.len() >= self.max_sessions {
                warn!(
                    current = inner.sessions.len(),
                    max = self.max_sessions,
                    "max sessions limit reached"
                );
                return Err(ServerError::MaxSessionsReached);
            }
        }

        // Create new connection
        let connection = self.factory.create_connection()?;

        // Create session with the specified ID
        let session = Arc::new(Session::new_with_id(
            session_id.clone(),
            connection,
            self.writes_enabled,
        ));

        // Register session
        {
            let mut inner = self.inner.write().expect("registry lock poisoned");
            inner.sessions.insert(session_id.clone(), session.clone());
            info!(
                session_id = %session_id,
                total_sessions = inner.sessions.len(),
                "session created with specific ID"
            );
        }

        Ok(session)
    }
}
