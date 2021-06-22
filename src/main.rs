use futures::TryStreamExt;
use http::StatusCode;
use pin_project::pin_project;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fmt::{self, Display},
    net::Ipv6Addr,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::broadcast;
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    StreamExt,
};
use warp::{filters::sse, path, reject, reply, sse::Event, Filter, Stream};

#[derive(Clone, Debug, Deserialize)]
struct IdentifiedPayload {
    id: Arc<str>,
    payload: Arc<str>, // base64
}

struct Session {
    _created_at: Instant,
    initial_payload: Option<Arc<str>>,
}

struct Room {
    users: Mutex<HashMap<Arc<str>, Session>>,
    updates: broadcast::Sender<IdentifiedPayload>,
    _keep_open: broadcast::Receiver<IdentifiedPayload>,
}

#[derive(Default)]
struct State {
    rooms: Mutex<HashMap<String, Arc<Room>>>,
}

#[pin_project]
struct WaitOnceStream<F>(#[pin] Option<F>);

#[derive(Debug)]
struct Shutdown;

impl State {
    pub fn get_or_make_room(&self, id: String) -> Arc<Room> {
        let mut rooms = self.rooms.lock().unwrap();
        let room = rooms.entry(id).or_insert_with(|| {
            let (tx, rx) = broadcast::channel(10);
            Arc::new(Room {
                updates: tx,
                _keep_open: rx,
                users: Default::default(),
            })
        });
        Arc::clone(&room)
    }

    pub fn get_room(&self, id: &str) -> Option<Arc<Room>> {
        let rooms = self.rooms.lock().unwrap();
        rooms.get(id).cloned()
    }
}

impl Room {
    pub fn start_session(&self, id: Arc<str>, payload: Arc<str>) {
        // create user session entry
        let user = Session {
            _created_at: Instant::now(),
            initial_payload: Some(payload),
        };
        let mut users = self.users.lock().unwrap();
        // will terminate previous stream if it existed
        users.insert(id, user);
    }

    pub fn stream(
        &self,
        session: String,
    ) -> Option<impl Stream<Item = Result<Event, BroadcastStreamRecvError>>> {
        // existence check
        {
            let mut users = self.users.lock().unwrap();
            let user = users.get_mut(&*session)?;
            if let Some(payload) = user.initial_payload.take() {
                self.updates
                    .send(IdentifiedPayload {
                        id: session.clone().into(),
                        payload,
                    })
                    .unwrap();
            }
        }

        let rx = self.updates.subscribe();
        let stream = BroadcastStream::new(rx)
            .filter(move |op| {
                op.as_ref()
                    .ok()
                    .filter(|IdentifiedPayload { id, .. }| id.as_ref() != session.as_str())
                    .is_some()
            })
            .map_ok(|IdentifiedPayload { payload, .. }| {
                Event::default().event("client").data(&*payload)
            })
            .fuse();
        Some(stream)
    }
}

#[tokio::main]
async fn main() {
    let state = Arc::new(State::default());

    let rooms = path("rooms").and(path::param());

    let state_join = Arc::clone(&state);
    let join = rooms
        .and(path::end())
        .and(warp::post())
        .and(warp::body::json())
        .map(move |room, IdentifiedPayload { id, payload }| {
            let room = state_join.get_or_make_room(room);
            room.start_session(id, payload);
            reply::with_status(reply(), StatusCode::CREATED)
        });

    let state_update = Arc::clone(&state);
    let update = rooms.and(warp::put()).and(warp::body::json()).map(
        move |room: String, payload: IdentifiedPayload| {
            let room = state_update.get_room(&room);
            if let Some(room) = room {
                room.updates.send(payload).unwrap();
                reply::with_status(reply::reply(), StatusCode::ACCEPTED)
            } else {
                reply::with_status(reply::reply(), StatusCode::NOT_FOUND)
            }
        },
    );

    let state_session = Arc::clone(&state);
    let session = rooms
        .and(path::param())
        .and(path::end())
        .and(warp::get())
        .and_then(move |room: String, session: String| {
            let res = state_session
                .get_room(&room)
                .and_then(move |room| room.stream(session))
                .map(|stream| sse::reply(sse::keep_alive().stream(stream)))
                .ok_or_else(|| reject::not_found());
            futures::future::ready(res)
        });

    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods([http::Method::POST, http::Method::PUT])
        .allow_header(http::header::CONTENT_TYPE);

    let routes = join.or(update).or(session).with(cors);

    let addr = (Ipv6Addr::UNSPECIFIED, 8080);
    warp::serve(routes).run(addr).await;
}

impl Display for Shutdown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Shutdown")
    }
}

impl std::error::Error for Shutdown {}
