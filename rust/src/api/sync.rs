use reqwest::{Client, Method, header::{HeaderMap, HeaderValue, AUTHORIZATION}};
use once_cell::sync::Lazy;
use quick_xml::Reader;
use quick_xml::events::Event;
use std::path::Path;
use serde::{Deserialize, Serialize};
use base64::{Engine as _, engine::general_purpose::STANDARD as b64};
use parking_lot::Mutex;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;
use std::io::Write;
use chrono::Utc;

use crate::api::models::{CloudBook, SyncResult, ProgressSyncResult, SyncProgressEvent, SyncHistoryEntry, Book, Chapter, ReadingProgress, Bookmark, Highlight, BookBookmarksFile, BookHighlightsFile};

const SECRET_KEY_WEBDAV_PASSWORD: &str = "webdav_password";

// --- Password Storage ---

pub fn save_webdav_password(password: String) -> Result<(), String> {
    crate::api::database::set_secret(SECRET_KEY_WEBDAV_PASSWORD, &password)
}

pub fn get_webdav_password() -> Result<Option<String>, String> {
    crate::api::database::get_secret(SECRET_KEY_WEBDAV_PASSWORD)
}

pub fn delete_webdav_password() -> Result<(), String> {
    crate::api::database::delete_secret(SECRET_KEY_WEBDAV_PASSWORD)
}

use crate::frb_generated::StreamSink;

// --- Stream Events ---

static SYNC_EVENT_SINK: Lazy<Mutex<Option<StreamSink<SyncProgressEvent>>>> =
    Lazy::new(|| Mutex::new(None));

pub fn subscribe_sync_events(sink: StreamSink<SyncProgressEvent>) -> Result<(), String> {
    let mut lock = SYNC_EVENT_SINK.lock();
    *lock = Some(sink);
    Ok(())
}

fn emit_sync_event(event: SyncProgressEvent) {
    let lock = SYNC_EVENT_SINK.lock();
    if let Some(ref sink) = *lock {
        let _ = sink.add(event);
    }
}

fn record_sync_success_timestamp() {
    if let Ok(Some(mut settings)) = crate::api::database::get_settings() {
        settings.web_dav_last_sync = Some(Utc::now().timestamp_millis());
        let _ = crate::api::database::save_settings(settings);
    }
}

// --- WebDAV Client & XML ---

#[derive(Debug, Serialize, Deserialize)]
pub struct WebDavFile {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size: i64,
    pub last_modified: String,
}

#[derive(Clone)]
pub struct WebDavClient {
    client: Client,
    base_url: String,
    auth_header: String,
}

impl WebDavClient {
    pub fn new(url: &str, user: &str, pass: &str) -> Result<Self, String> {
        let mut formatted_url = url.trim().to_string();
        if !formatted_url.starts_with("http://") && !formatted_url.starts_with("https://") {
            formatted_url = format!("https://{}", formatted_url);
        }
        
        let auth = format!("{}:{}", user.trim(), pass);
        let auth_b64 = b64.encode(auth);
        let auth_header = format!("Basic {}", auth_b64);
        
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .connect_timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|e| format!("Failed to build HTTP client: {}", e))?;

        Ok(WebDavClient {
            client,
            base_url: formatted_url,
            auth_header,
        })
    }
    
    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Ok(val) = HeaderValue::from_str(&self.auth_header) {
            h.insert(AUTHORIZATION, val);
        }
        h
    }

    pub async fn test_connection(&self) -> Result<bool, String> {
        let propfind = Method::from_bytes(b"PROPFIND").map_err(|e| e.to_string())?;
        let mut h = self.headers();
        h.insert("Depth", HeaderValue::from_static("0"));
        
        let res = self.client.request(propfind, &self.base_url)
            .headers(h)
            .send()
            .await
            .map_err(|e| e.to_string())?;
            
        Ok(res.status().is_success() || res.status().as_u16() == 207)
    }

    pub async fn mkdir(&self, remote_path: &str) -> Result<bool, String> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), remote_path.trim_start_matches('/'));
        let mkcol = Method::from_bytes(b"MKCOL").map_err(|e| e.to_string())?;
        
        let res = self.client.request(mkcol, &url)
            .headers(self.headers())
            .send()
            .await
            .map_err(|e| e.to_string())?;
            
        Ok(res.status().is_success() || res.status().as_u16() == 405)
    }

    pub async fn upload_bytes(&self, remote_path: &str, bytes: Vec<u8>) -> Result<bool, String> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), remote_path.trim_start_matches('/'));
        
        let res = self.client.put(&url)
            .headers(self.headers())
            .body(bytes)
            .send()
            .await
            .map_err(|e| e.to_string())?;
            
        if !res.status().is_success() {
            return Err(format!("Upload failed for '{}' with status: {}", remote_path, res.status()));
        }
        Ok(true)
    }

    pub async fn download_bytes(&self, remote_path: &str) -> Result<Vec<u8>, String> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), remote_path.trim_start_matches('/'));
        
        let res = self.client.get(&url)
            .headers(self.headers())
            .send()
            .await
            .map_err(|e| e.to_string())?;
            
        if !res.status().is_success() {
            return Err(format!("Download failed for '{}' with status: {}", remote_path, res.status()));
        }
        
        let bytes = res.bytes().await.map_err(|e| e.to_string())?;
        if bytes.is_empty() {
            return Err(format!("File '{}' on WebDAV is empty (0 bytes)", remote_path));
        }
        Ok(bytes.to_vec())
    }

    pub async fn upload_file(&self, remote_path: &str, local_path: &str) -> Result<bool, String> {
        let path = Path::new(local_path);
        if !path.exists() {
            return Err(format!("Local file does not exist: {}", local_path));
        }
        
        let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
        self.upload_bytes(remote_path, bytes).await
    }

    pub async fn download_file(&self, remote_path: &str, local_path: &str) -> Result<bool, String> {
        let bytes = self.download_bytes(remote_path).await?;
        
        let path = Path::new(local_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        
        std::fs::write(path, bytes).map_err(|e| e.to_string())?;
        Ok(true)
    }

    pub async fn list_files(&self, remote_path: &str) -> Result<Vec<WebDavFile>, String> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), remote_path.trim_start_matches('/'));
        let propfind = Method::from_bytes(b"PROPFIND").map_err(|e| e.to_string())?;
        let mut h = self.headers();
        h.insert("Depth", HeaderValue::from_static("1"));
        
        let res = self.client.request(propfind, &url)
            .headers(h)
            .send()
            .await
            .map_err(|e| e.to_string())?;
            
        if !res.status().is_success() && res.status().as_u16() != 207 {
            return Err(format!("PROPFIND failed with status: {}", res.status()));
        }
        
        let xml_text = res.text().await.map_err(|e| e.to_string())?;
        parse_webdav_xml(&xml_text)
    }

    pub async fn remove(&self, remote_path: &str) -> Result<bool, String> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), remote_path.trim_start_matches('/'));
        
        let res = self.client.delete(&url)
            .headers(self.headers())
            .send()
            .await
            .map_err(|e| e.to_string())?;
            
        Ok(res.status().is_success())
    }

    pub async fn get_file_size(&self, remote_path: &str) -> Result<Option<i64>, String> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), remote_path.trim_start_matches('/'));
        let propfind = Method::from_bytes(b"PROPFIND").map_err(|e| e.to_string())?;
        
        let res = self.client.request(propfind, &url)
            .headers(self.headers())
            .header("Depth", "0")
            .send()
            .await
            .map_err(|e| e.to_string())?;
            
        if !res.status().is_success() && res.status().as_u16() != 207 {
            return Ok(None);
        }

        let xml_text = res.text().await.map_err(|e| e.to_string())?;
        let files = parse_webdav_xml(&xml_text)?;
        if let Some(f) = files.into_iter().next() {
            Ok(Some(f.size))
        } else {
            Ok(None)
        }
    }

    pub async fn file_exists(&self, remote_path: &str) -> Result<bool, String> {
        let size = self.get_file_size(remote_path).await?;
        Ok(size.is_some())
    }

    pub async fn file_exists_and_non_empty(&self, remote_path: &str) -> Result<bool, String> {
        let size = self.get_file_size(remote_path).await?;
        Ok(size.map(|s| s > 0).unwrap_or(false))
    }
}

fn parse_webdav_xml(xml: &str) -> Result<Vec<WebDavFile>, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    
    let mut files = Vec::new();
    let mut buf = Vec::new();
    
    let mut current_href = String::new();
    let mut current_displayname = String::new();
    let mut current_size: i64 = 0;
    let mut current_last_modified = String::new();
    let mut is_dir = false;
    
    let mut in_response = false;
    let mut current_tag = String::new();
    
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name_bytes = e.local_name();
                let tag_name = String::from_utf8_lossy(name_bytes.as_ref()).to_lowercase();
                if tag_name == "response" {
                    in_response = true;
                    current_href.clear();
                    current_displayname.clear();
                    current_size = 0;
                    current_last_modified.clear();
                    is_dir = false;
                }
                if tag_name == "collection" && in_response {
                    is_dir = true;
                }
                current_tag = tag_name;
            }
            Ok(Event::Empty(ref e)) => {
                let name_bytes = e.local_name();
                let tag_name = String::from_utf8_lossy(name_bytes.as_ref()).to_lowercase();
                if tag_name == "collection" && in_response {
                    is_dir = true;
                }
            }
            Ok(Event::Text(e)) => {
                if in_response {
                    let text = e.unescape().unwrap_or_default().trim().to_string();
                    match current_tag.as_str() {
                        "href" => current_href = text,
                        "displayname" => current_displayname = text,
                        "getcontentlength" => current_size = text.parse().unwrap_or(0),
                        "getlastmodified" => current_last_modified = text,
                        _ => {}
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let name_bytes = e.local_name();
                let tag_name = String::from_utf8_lossy(name_bytes.as_ref()).to_lowercase();
                if tag_name == "response" {
                    in_response = false;
                    let name = if !current_displayname.is_empty() {
                        current_displayname.clone()
                    } else {
                        current_href.trim_end_matches('/').split('/').last().unwrap_or("").to_string()
                    };
                    if !name.is_empty() {
                        files.push(WebDavFile {
                            name,
                            path: current_href.clone(),
                            is_dir,
                            size: current_size,
                            last_modified: current_last_modified.clone(),
                        });
                    }
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(format!("XML error at position {}: {:?}", reader.buffer_position(), e)),
            _ => {}
        }
        buf.clear();
    }
    
    Ok(files)
}

static WEBDAV_CLIENT: Lazy<Mutex<Option<WebDavClient>>> = Lazy::new(|| Mutex::new(None));

// --- Config Helpers ---

pub fn save_webdav_config(url: String, username: String, password: Option<String>) -> Result<(), String> {
    if let Some(mut settings) = crate::api::database::get_settings()? {
        settings.web_dav_url = url.trim().to_string();
        settings.web_dav_username = username.trim().to_string();
        settings.web_dav_enabled = !settings.web_dav_url.is_empty() && !settings.web_dav_username.is_empty();
        crate::api::database::save_settings(settings)?;
    }

    if let Some(pass) = password {
        save_webdav_password(pass)?;
    }
    Ok(())
}

pub async fn get_or_init_client() -> Result<WebDavClient, String> {
    let settings_opt = crate::api::database::get_settings()?;
    let settings = settings_opt.ok_or_else(|| "App settings not initialized".to_string())?;
    if !settings.web_dav_enabled || settings.web_dav_url.is_empty() || settings.web_dav_username.is_empty() {
        return Err("WebDAV is not configured or disabled".to_string());
    }
    let password = get_webdav_password()?.unwrap_or_default();
    if password.is_empty() {
        return Err("WebDAV password is not set".to_string());
    }
    let client = WebDavClient::new(&settings.web_dav_url, &settings.web_dav_username, &password)?;
    let mut cache = WEBDAV_CLIENT.lock();
    *cache = Some(client.clone());
    Ok(client)
}

pub async fn test_webdav_connection(
    url: Option<String>,
    username: Option<String>,
    password: Option<String>,
) -> Result<bool, String> {
    let client = if let (Some(u), Some(user), Some(pass)) = (url, username, password) {
        WebDavClient::new(&u, &user, &pass)?
    } else {
        get_or_init_client().await?
    };
    client.test_connection().await
}

// --- Sync Payloads & JSON Structs ---

#[derive(Debug, Serialize, Deserialize)]
struct SyncBookPayload {
    uuid: String,
    title: String,
    author: String,
    #[serde(rename = "totalChapters")]
    total_chapters: i32,
    #[serde(rename = "coverExtension")]
    cover_extension: Option<String>,
    #[serde(rename = "dateAdded")]
    date_added: String,
    chapters: Vec<SyncChapterPayload>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncChapterPayload {
    #[serde(rename = "chapterIndex")]
    chapter_index: i32,
    title: String,
    paragraphs: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncProgressPayload {
    #[serde(rename = "bookUuid")]
    book_uuid: String,
    #[serde(rename = "chapterIndex")]
    chapter_index: i32,
    #[serde(rename = "paragraphIndex")]
    paragraph_index: i32,
    #[serde(rename = "characterOffset")]
    character_offset: i32,
    #[serde(rename = "lastRead")]
    last_read: i64,
    #[serde(rename = "deviceId")]
    device_id: Option<String>,
    #[serde(rename = "deviceName")]
    device_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncDataFile {
    version: i32,
    #[serde(rename = "lastSyncTime")]
    last_sync_time: String,
    books: Vec<SyncDataBookItem>,
    #[serde(default)]
    deleted: Vec<SyncDeletedItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncDataBookItem {
    uuid: String,
    title: String,
    author: String,
    #[serde(rename = "totalChapters")]
    total_chapters: i32,
    #[serde(rename = "dateAdded")]
    date_added: String,
    #[serde(rename = "coverExtension")]
    cover_extension: Option<String>,
    #[serde(rename = "hasCover")]
    has_cover: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum SyncDeletedItem {
    Simple(String),
    Detailed {
        uuid: String,
        #[serde(rename = "deletedAt")]
        deleted_at: String,
    },
}

impl SyncDeletedItem {
    fn uuid(&self) -> &str {
        match self {
            SyncDeletedItem::Simple(u) => u,
            SyncDeletedItem::Detailed { uuid, .. } => uuid,
        }
    }
}

// --- High-Level Sync APIs ---

pub async fn export_and_upload_book(book_uuid: String) -> Result<bool, String> {
    let book = crate::api::database::get_book_by_uuid(book_uuid.clone())?
        .ok_or_else(|| format!("Book not found: {}", book_uuid))?;
    let chapters = crate::api::database::get_chapters(book_uuid.clone())?;

    let date_added_iso = chrono::DateTime::from_timestamp_millis(book.date_added)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339());

    let cover_extension = book.cover_path.as_ref().map(|p| {
        Path::new(p)
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| format!(".{}", ext))
            .unwrap_or_else(|| ".jpg".to_string())
    });

    let sync_chapters = chapters.into_iter().map(|c| SyncChapterPayload {
        chapter_index: c.chapter_index,
        title: c.title,
        paragraphs: c.paragraphs,
    }).collect();

    let payload = SyncBookPayload {
        uuid: book.uuid.clone(),
        title: book.title,
        author: book.author,
        total_chapters: book.total_chapters,
        cover_extension,
        date_added: date_added_iso,
        chapters: sync_chapters,
    };

    let json_bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&json_bytes).map_err(|e| e.to_string())?;
    let compressed_bytes = encoder.finish().map_err(|e| e.to_string())?;

    let client = get_or_init_client().await?;
    let _ = client.mkdir("/AudireReader").await;
    let _ = client.mkdir("/AudireReader/books").await;
    let _ = client.mkdir("/AudireReader/covers").await;
    let _ = client.mkdir("/AudireReader/progress").await;
    let _ = client.mkdir("/AudireReader/bookmarks").await;
    let _ = client.mkdir("/AudireReader/highlights").await;

    let remote_path = format!("/AudireReader/books/{}.json.gz", book_uuid);
    client.upload_bytes(&remote_path, compressed_bytes).await?;

    if let Some(ref cover_path_str) = book.cover_path {
        let cover_path = Path::new(cover_path_str);
        if cover_path.exists() {
            let ext = cover_path.extension().and_then(|s| s.to_str()).unwrap_or("jpg");
            let remote_cover = format!("/AudireReader/covers/{}.{}", book_uuid, ext);
            let _ = client.upload_file(&remote_cover, cover_path_str).await;
        }
    }

    Ok(true)
}

pub async fn download_and_import_book(book_uuid: String, documents_dir: String) -> Result<Book, String> {
    let client = get_or_init_client().await?;

    let possible_paths = [
        format!("/AudireReader/books/{}.json.gz", book_uuid),
        format!("/AudireReader/books/{}.json", book_uuid),
        format!("/NovelReader/books/{}.json.gz", book_uuid),
        format!("/NovelReader/books/{}.json", book_uuid),
    ];

    let mut downloaded_bytes = None;
    for remote_path in &possible_paths {
        if let Ok(bytes) = client.download_bytes(remote_path).await {
            if !bytes.is_empty() {
                downloaded_bytes = Some(bytes);
                break;
            }
        }
    }

    let bytes = downloaded_bytes.ok_or_else(|| {
        format!("File nội dung truyện '{}' trên WebDAV bị thiếu hoặc rỗng (0 bytes). Vui lòng mở thiết bị có truyện và bấm 'Đồng bộ' (hoặc 'Đẩy lên Cloud') để tải lại nội dung.", book_uuid)
    })?;

    let json_bytes = if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(&bytes[..]);
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut decompressed)
            .map_err(|e| format!("Failed to decompress gzip book data: {}", e))?;
        decompressed
    } else {
        bytes
    };

    if json_bytes.is_empty() {
        return Err(format!("Decoded JSON data for book '{}' is empty (0 bytes)", book_uuid));
    }

    let payload: SyncBookPayload = serde_json::from_slice(&json_bytes)
        .map_err(|e| format!("Failed to parse book JSON ({} bytes received): {}", json_bytes.len(), e))?;

    let mut cover_path = None;
    if let Some(ref ext) = payload.cover_extension {
        let ext_with_dot = if ext.starts_with('.') { ext.clone() } else { format!(".{}", ext) };
        let remote_cover = format!("/AudireReader/covers/{}{}", book_uuid, ext_with_dot);
        if let Ok(exists) = client.file_exists(&remote_cover).await {
            if exists {
                let local_cover_dir = Path::new(&documents_dir).join("covers");
                if !local_cover_dir.exists() {
                    let _ = std::fs::create_dir_all(&local_cover_dir);
                }
                let local_cover_path = local_cover_dir.join(format!("{}{}", book_uuid, ext_with_dot));
                let local_cover_str = local_cover_path.to_string_lossy().to_string();
                if client.download_file(&remote_cover, &local_cover_str).await.is_ok() {
                    cover_path = Some(local_cover_str);
                }
            }
        }
    }

    let date_added_millis = chrono::DateTime::parse_from_rfc3339(&payload.date_added)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|_| Utc::now().timestamp_millis());

    let book = Book {
        id: None,
        uuid: payload.uuid.clone(),
        title: payload.title,
        author: payload.author,
        cover_path,
        total_chapters: payload.total_chapters,
        date_added: date_added_millis,
        status: "unread".to_string(),
        tags: vec![],
    };

    let chapters: Vec<Chapter> = payload.chapters.into_iter().map(|c| Chapter {
        id: None,
        book_uuid: payload.uuid.clone(),
        chapter_index: c.chapter_index,
        title: c.title,
        paragraphs: c.paragraphs,
    }).collect();

    crate::api::database::insert_book(book.clone())?;
    crate::api::database::insert_chapters(chapters)?;

    if let Ok(conn) = crate::api::database::get_conn() {
        let _ = conn.execute_batch("PRAGMA shrink_memory;");
    }

    Ok(book)
}

pub async fn fetch_cloud_books(documents_dir: Option<String>) -> Result<Vec<CloudBook>, String> {
    let client = get_or_init_client().await?;
    
    if !client.file_exists("/AudireReader/sync_data.json").await.unwrap_or(false) {
        return Ok(Vec::new());
    }

    let bytes = client.download_bytes("/AudireReader/sync_data.json").await?;
    let sync_data: SyncDataFile = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;

    let cloud_books: Vec<CloudBook> = sync_data.books.into_iter().map(|b| CloudBook {
        uuid: b.uuid,
        title: b.title,
        author: b.author,
        total_chapters: b.total_chapters,
        cover_extension: b.cover_extension,
        has_cover: b.has_cover,
        date_added: b.date_added,
    }).collect();

    // Background download missing covers if documents_dir provided
    if let Some(doc_dir) = documents_dir {
        let books_clone = cloud_books.clone();
        let client_clone = client.clone();
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(r) => r,
                Err(_) => return,
            };
            rt.block_on(async move {
                for cb in books_clone {
                    if cb.has_cover {
                        let ext = cb.cover_extension.unwrap_or_else(|| ".jpg".to_string());
                        let local_path = Path::new(&doc_dir).join("covers").join(format!("{}{}", cb.uuid, ext));
                        if !local_path.exists() {
                            let remote_cover = format!("/AudireReader/covers/{}{}", cb.uuid, ext);
                            let _ = client_clone.download_file(&remote_cover, &local_path.to_string_lossy()).await;
                        }
                    }
                }
            });
        });
    }

    Ok(cloud_books)
}

fn record_book_progress_sync(book_uuid: &str, action: &str, chapter_idx: i32, para_idx: i32, device: Option<String>) {
    let book_title = crate::api::database::get_book_by_uuid(book_uuid.to_string())
        .ok()
        .flatten()
        .map(|b| b.title)
        .unwrap_or_else(|| "Unknown Book".to_string());

    let details_json = serde_json::json!({
        "bookTitle": book_title,
        "chapterIndex": chapter_idx,
        "paragraphIndex": para_idx,
        "deviceName": device.unwrap_or_else(|| "This Device".to_string()),
    }).to_string();

    let _ = crate::api::database::insert_sync_history(SyncHistoryEntry {
        id: None,
        timestamp: Utc::now().timestamp_millis(),
        action: action.to_string(),
        status: "success".to_string(),
        details: details_json,
    });
}

pub async fn sync_book_progress(book_uuid: String) -> Result<ProgressSyncResult, String> {
    let client = get_or_init_client().await?;
    let remote_path = format!("/AudireReader/progress/{}.json", book_uuid);

    let local_prog = crate::api::database::get_reading_progress(book_uuid.clone())?;

    let cloud_prog_opt: Option<SyncProgressPayload> = if client.file_exists(&remote_path).await.unwrap_or(false) {
        if let Ok(bytes) = client.download_bytes(&remote_path).await {
            serde_json::from_slice(&bytes).ok()
        } else {
            None
        }
    } else {
        None
    };

    match (local_prog, cloud_prog_opt) {
        (None, None) => Ok(ProgressSyncResult {
            status: "noChange".to_string(),
            cloud_chapter_index: None,
            cloud_paragraph_index: None,
            cloud_character_offset: None,
            cloud_last_read: None,
            message: None,
        }),
        (Some(local), None) => {
            let payload = SyncProgressPayload {
                book_uuid: book_uuid.clone(),
                chapter_index: local.current_chapter_index,
                paragraph_index: local.current_paragraph_index,
                character_offset: local.current_character_offset,
                last_read: local.last_read,
                device_id: None,
                device_name: None,
            };
            let json_bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
            let _ = client.upload_bytes(&remote_path, json_bytes).await;
            record_book_progress_sync(&book_uuid, "push", local.current_chapter_index, local.current_paragraph_index, None);
            record_sync_success_timestamp();
            Ok(ProgressSyncResult {
                status: "updatedCloud".to_string(),
                cloud_chapter_index: Some(local.current_chapter_index),
                cloud_paragraph_index: Some(local.current_paragraph_index),
                cloud_character_offset: Some(local.current_character_offset),
                cloud_last_read: Some(local.last_read),
                message: Some("Uploaded local progress to cloud".to_string()),
            })
        }
        (None, Some(cloud)) => {
            let prog = ReadingProgress {
                id: None,
                book_uuid: book_uuid.clone(),
                current_chapter_index: cloud.chapter_index,
                current_paragraph_index: cloud.paragraph_index,
                current_character_offset: cloud.character_offset,
                last_read: cloud.last_read,
            };
            crate::api::database::save_reading_progress(prog)?;
            record_book_progress_sync(&book_uuid, "pull", cloud.chapter_index, cloud.paragraph_index, cloud.device_name.clone());
            record_sync_success_timestamp();
            Ok(ProgressSyncResult {
                status: "updatedLocal".to_string(),
                cloud_chapter_index: Some(cloud.chapter_index),
                cloud_paragraph_index: Some(cloud.paragraph_index),
                cloud_character_offset: Some(cloud.character_offset),
                cloud_last_read: Some(cloud.last_read),
                message: Some("Updated local progress from cloud".to_string()),
            })
        }
        (Some(local), Some(cloud)) => {
            if local.last_read > cloud.last_read {
                let payload = SyncProgressPayload {
                    book_uuid: book_uuid.clone(),
                    chapter_index: local.current_chapter_index,
                    paragraph_index: local.current_paragraph_index,
                    character_offset: local.current_character_offset,
                    last_read: local.last_read,
                    device_id: None,
                    device_name: None,
                };
                let json_bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
                let _ = client.upload_bytes(&remote_path, json_bytes).await;
                record_book_progress_sync(&book_uuid, "push", local.current_chapter_index, local.current_paragraph_index, None);
                record_sync_success_timestamp();
                Ok(ProgressSyncResult {
                    status: "updatedCloud".to_string(),
                    cloud_chapter_index: Some(local.current_chapter_index),
                    cloud_paragraph_index: Some(local.current_paragraph_index),
                    cloud_character_offset: Some(local.current_character_offset),
                    cloud_last_read: Some(local.last_read),
                    message: Some("Local is newer, updated cloud".to_string()),
                })
            } else if cloud.last_read > local.last_read {
                let prog = ReadingProgress {
                    id: None,
                    book_uuid: book_uuid.clone(),
                    current_chapter_index: cloud.chapter_index,
                    current_paragraph_index: cloud.paragraph_index,
                    current_character_offset: cloud.character_offset,
                    last_read: cloud.last_read,
                };
                crate::api::database::save_reading_progress(prog)?;
                record_book_progress_sync(&book_uuid, "pull", cloud.chapter_index, cloud.paragraph_index, cloud.device_name.clone());
                record_sync_success_timestamp();
                Ok(ProgressSyncResult {
                    status: "updatedLocal".to_string(),
                    cloud_chapter_index: Some(cloud.chapter_index),
                    cloud_paragraph_index: Some(cloud.paragraph_index),
                    cloud_character_offset: Some(cloud.character_offset),
                    cloud_last_read: Some(cloud.last_read),
                    message: Some("Cloud is newer, updated local".to_string()),
                })
            } else {
                record_sync_success_timestamp();
                Ok(ProgressSyncResult {
                    status: "noChange".to_string(),
                    cloud_chapter_index: Some(cloud.chapter_index),
                    cloud_paragraph_index: Some(cloud.paragraph_index),
                    cloud_character_offset: Some(cloud.character_offset),
                    cloud_last_read: Some(cloud.last_read),
                    message: None,
                })
            }
        }
    }
}

pub async fn sync_book_bookmarks(book_uuid: String) -> Result<bool, String> {
    let client = get_or_init_client().await?;
    let _ = client.mkdir("/AudireReader/bookmarks").await;
    let remote_path = format!("/AudireReader/bookmarks/{}.json", book_uuid);

    let local_bookmarks = crate::api::database::get_bookmarks(book_uuid.clone())?;

    let cloud_file_opt: Option<BookBookmarksFile> = if client.file_exists(&remote_path).await.unwrap_or(false) {
        if let Ok(bytes) = client.download_bytes(&remote_path).await {
            serde_json::from_slice(&bytes).ok()
        } else {
            None
        }
    } else {
        None
    };

    let mut merged_map: std::collections::HashMap<(i32, i32), Bookmark> = std::collections::HashMap::new();

    // 1. Add all local bookmarks
    for b in local_bookmarks {
        merged_map.insert((b.chapter_index, b.paragraph_index), b);
    }

    let mut local_updated = false;

    // 2. Merge cloud bookmarks
    if let Some(cloud_file) = cloud_file_opt {
        for cb in cloud_file.bookmarks {
            let key = (cb.chapter_index, cb.paragraph_index);
            if let Some(existing) = merged_map.get_mut(&key) {
                if cb.date_added > existing.date_added {
                    *existing = cb;
                    local_updated = true;
                }
            } else {
                merged_map.insert(key, cb);
                local_updated = true;
            }
        }
    }

    let mut final_bookmarks: Vec<Bookmark> = merged_map.into_values().collect();
    final_bookmarks.sort_by(|a, b| {
        a.chapter_index.cmp(&b.chapter_index).then(a.paragraph_index.cmp(&b.paragraph_index))
    });

    if local_updated {
        crate::api::database::replace_all_bookmarks(book_uuid.clone(), final_bookmarks.clone())?;
    }

    // Upload merged result to cloud
    let payload = BookBookmarksFile {
        book_uuid: book_uuid.clone(),
        updated_at: Utc::now().timestamp_millis(),
        bookmarks: final_bookmarks,
    };
    let json_bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    let _ = client.upload_bytes(&remote_path, json_bytes).await;

    Ok(local_updated)
}

pub async fn sync_book_highlights(book_uuid: String) -> Result<bool, String> {
    let client = get_or_init_client().await?;
    let _ = client.mkdir("/AudireReader/highlights").await;
    let remote_path = format!("/AudireReader/highlights/{}.json", book_uuid);

    let local_highlights = crate::api::database::get_highlights(book_uuid.clone())?;

    let cloud_file_opt: Option<BookHighlightsFile> = if client.file_exists(&remote_path).await.unwrap_or(false) {
        if let Ok(bytes) = client.download_bytes(&remote_path).await {
            serde_json::from_slice(&bytes).ok()
        } else {
            None
        }
    } else {
        None
    };

    let mut merged_map: std::collections::HashMap<(i32, i32, Option<i32>, Option<i32>), Highlight> = std::collections::HashMap::new();

    // 1. Add all local highlights
    for h in local_highlights {
        merged_map.insert((h.chapter_index, h.paragraph_index, h.start_offset, h.end_offset), h);
    }

    let mut local_updated = false;

    // 2. Merge cloud highlights
    if let Some(cloud_file) = cloud_file_opt {
        for ch in cloud_file.highlights {
            let key = (ch.chapter_index, ch.paragraph_index, ch.start_offset, ch.end_offset);
            if let Some(existing) = merged_map.get_mut(&key) {
                if ch.date_added > existing.date_added {
                    *existing = ch;
                    local_updated = true;
                }
            } else {
                merged_map.insert(key, ch);
                local_updated = true;
            }
        }
    }

    let mut final_highlights: Vec<Highlight> = merged_map.into_values().collect();
    final_highlights.sort_by(|a, b| {
        a.chapter_index.cmp(&b.chapter_index)
            .then(a.paragraph_index.cmp(&b.paragraph_index))
            .then(a.start_offset.cmp(&b.start_offset))
    });

    if local_updated {
        crate::api::database::replace_all_highlights(book_uuid.clone(), final_highlights.clone())?;
    }

    // Upload merged result to cloud
    let payload = BookHighlightsFile {
        book_uuid: book_uuid.clone(),
        updated_at: Utc::now().timestamp_millis(),
        highlights: final_highlights,
    };
    let json_bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    let _ = client.upload_bytes(&remote_path, json_bytes).await;

    Ok(local_updated)
}

pub async fn sync_library(documents_dir: Option<String>) -> Result<SyncResult, String> {
    emit_sync_event(SyncProgressEvent {
        event_type: "syncStarted".to_string(),
        book_uuid: None,
        status: Some("syncing".to_string()),
        current: 0,
        total: 100,
        message: Some("Initializing WebDAV sync...".to_string()),
    });

    let client = get_or_init_client().await?;
    
    // Ensure remote folders
    let _ = client.mkdir("/AudireReader").await;
    let _ = client.mkdir("/AudireReader/covers").await;
    let _ = client.mkdir("/AudireReader/books").await;
    let _ = client.mkdir("/AudireReader/progress").await;
    let _ = client.mkdir("/AudireReader/bookmarks").await;
    let _ = client.mkdir("/AudireReader/highlights").await;

    // Migrate from legacy NovelReader schema if needed
    if client.file_exists("/NovelReader/sync_data.json").await.unwrap_or(false) 
        && !client.file_exists("/AudireReader/sync_data.json").await.unwrap_or(false) {
        if let Ok(old_bytes) = client.download_bytes("/NovelReader/sync_data.json").await {
            let _ = client.upload_bytes("/AudireReader/sync_data.json", old_bytes).await;
        }
    }

    let local_books = crate::api::database::get_all_books()?;
    let mut local_changed = false;

    // Retry loop for optimistic locking
    for attempt in 1..=3 {
        let mut cloud_sync_data = if client.file_exists("/AudireReader/sync_data.json").await.unwrap_or(false) {
            if let Ok(bytes) = client.download_bytes("/AudireReader/sync_data.json").await {
                serde_json::from_slice::<SyncDataFile>(&bytes).unwrap_or_else(|_| SyncDataFile {
                    version: 1,
                    last_sync_time: Utc::now().to_rfc3339(),
                    books: vec![],
                    deleted: vec![],
                })
            } else {
                SyncDataFile {
                    version: 1,
                    last_sync_time: Utc::now().to_rfc3339(),
                    books: vec![],
                    deleted: vec![],
                }
            }
        } else {
            SyncDataFile {
                version: 1,
                last_sync_time: Utc::now().to_rfc3339(),
                books: vec![],
                deleted: vec![],
            }
        };

        // Filter out tombstone books (>30 days deleted)
        let now = Utc::now();
        cloud_sync_data.deleted.retain(|item| {
            if let SyncDeletedItem::Detailed { deleted_at, .. } = item {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(deleted_at) {
                    return (now.naive_utc() - dt.naive_utc()).num_days() < 30;
                }
            }
            true
        });

        let deleted_uuids: std::collections::HashSet<String> = cloud_sync_data.deleted.iter().map(|d| d.uuid().to_string()).collect();

        // 1. Upload local books not present on cloud
        let mut cloud_uuids: std::collections::HashSet<String> = cloud_sync_data.books.iter().map(|b| b.uuid.clone()).collect();

        for (idx, local_book) in local_books.iter().enumerate() {
            if deleted_uuids.contains(&local_book.uuid) {
                // Book was deleted on cloud, delete locally
                let _ = crate::api::database::delete_book(local_book.uuid.clone());
                local_changed = true;
                continue;
            }

            let remote_file = format!("/AudireReader/books/{}.json.gz", local_book.uuid);
            let file_exists_on_cloud = client.file_exists_and_non_empty(&remote_file).await.unwrap_or(false);

            if !cloud_uuids.contains(&local_book.uuid) || !file_exists_on_cloud {
                emit_sync_event(SyncProgressEvent {
                    event_type: "bookStatus".to_string(),
                    book_uuid: Some(local_book.uuid.clone()),
                    status: Some("syncing".to_string()),
                    current: idx as i32 + 1,
                    total: local_books.len() as i32,
                    message: Some(format!("Uploading book: {}", local_book.title)),
                });

                if export_and_upload_book(local_book.uuid.clone()).await.unwrap_or(false) {
                    let ext = local_book.cover_path.as_ref().and_then(|p| Path::new(p).extension().and_then(|e| e.to_str()).map(|e| format!(".{}", e)));
                    if !cloud_uuids.contains(&local_book.uuid) {
                        cloud_sync_data.books.push(SyncDataBookItem {
                            uuid: local_book.uuid.clone(),
                            title: local_book.title.clone(),
                            author: local_book.author.clone(),
                            total_chapters: local_book.total_chapters,
                            date_added: chrono::DateTime::from_timestamp_millis(local_book.date_added).map(|dt| dt.to_rfc3339()).unwrap_or_else(|| Utc::now().to_rfc3339()),
                            cover_extension: ext,
                            has_cover: local_book.cover_path.is_some(),
                        });
                        cloud_uuids.insert(local_book.uuid.clone());
                    }
                }
            }
        }

        // 2. Upload updated sync_data.json
        cloud_sync_data.last_sync_time = Utc::now().to_rfc3339();
        let sync_bytes = serde_json::to_vec(&cloud_sync_data).map_err(|e| e.to_string())?;

        if client.upload_bytes("/AudireReader/sync_data.json", sync_bytes).await.unwrap_or(false) {
            break;
        } else if attempt == 3 {
            return Err("Failed to save sync index on WebDAV after 3 attempts".to_string());
        }
    }

    // 3. Sync all progress, bookmarks, and highlights
    for local_book in &local_books {
        let _ = sync_book_progress(local_book.uuid.clone()).await;
        let _ = sync_book_bookmarks(local_book.uuid.clone()).await;
        let _ = sync_book_highlights(local_book.uuid.clone()).await;
    }

    // Trigger background cover download if doc dir provided
    if let Some(doc_dir) = documents_dir {
        let _ = fetch_cloud_books(Some(doc_dir)).await;
    }

    let _ = crate::api::database::insert_sync_history(SyncHistoryEntry {
        id: None,
        timestamp: Utc::now().timestamp_millis(),
        action: "sync_library".to_string(),
        status: "success".to_string(),
        details: format!("Synced {} local books successfully", local_books.len()),
    });

    record_sync_success_timestamp();

    emit_sync_event(SyncProgressEvent {
        event_type: "syncFinished".to_string(),
        book_uuid: None,
        status: Some("success".to_string()),
        current: 100,
        total: 100,
        message: Some("Sync completed successfully".to_string()),
    });

    Ok(SyncResult {
        success: true,
        message: "Library and progress synchronized successfully".to_string(),
        local_changed,
    })
}

pub async fn sync_all(documents_dir: Option<String>) -> Result<SyncResult, String> {
    sync_library(documents_dir).await
}

pub async fn force_push(progress_only: bool) -> Result<SyncResult, String> {
    let local_books = crate::api::database::get_all_books()?;
    let client = get_or_init_client().await?;

    let _ = client.mkdir("/AudireReader").await;
    let _ = client.mkdir("/AudireReader/covers").await;
    let _ = client.mkdir("/AudireReader/books").await;
    let _ = client.mkdir("/AudireReader/progress").await;
    let _ = client.mkdir("/AudireReader/bookmarks").await;
    let _ = client.mkdir("/AudireReader/highlights").await;

    if !progress_only {
        let mut cloud_books = Vec::new();
        for local_book in &local_books {
            let _ = export_and_upload_book(local_book.uuid.clone()).await;
            let ext = local_book.cover_path.as_ref().and_then(|p| Path::new(p).extension().and_then(|e| e.to_str()).map(|e| format!(".{}", e)));
            cloud_books.push(SyncDataBookItem {
                uuid: local_book.uuid.clone(),
                title: local_book.title.clone(),
                author: local_book.author.clone(),
                total_chapters: local_book.total_chapters,
                date_added: chrono::DateTime::from_timestamp_millis(local_book.date_added).map(|dt| dt.to_rfc3339()).unwrap_or_else(|| Utc::now().to_rfc3339()),
                cover_extension: ext,
                has_cover: local_book.cover_path.is_some(),
            });
        }
        let sync_data = SyncDataFile {
            version: 1,
            last_sync_time: Utc::now().to_rfc3339(),
            books: cloud_books,
            deleted: vec![],
        };
        let sync_bytes = serde_json::to_vec(&sync_data).map_err(|e| e.to_string())?;
        client.upload_bytes("/AudireReader/sync_data.json", sync_bytes).await?;
    }

    for local_book in &local_books {
        if let Ok(Some(local_prog)) = crate::api::database::get_reading_progress(local_book.uuid.clone()) {
            let payload = SyncProgressPayload {
                book_uuid: local_book.uuid.clone(),
                chapter_index: local_prog.current_chapter_index,
                paragraph_index: local_prog.current_paragraph_index,
                character_offset: local_prog.current_character_offset,
                last_read: local_prog.last_read,
                device_id: None,
                device_name: None,
            };
            let json_bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
            let remote_path = format!("/AudireReader/progress/{}.json", local_book.uuid);
            let _ = client.upload_bytes(&remote_path, json_bytes).await;
        }

        // Upload local bookmarks & highlights
        if let Ok(bookmarks) = crate::api::database::get_bookmarks(local_book.uuid.clone()) {
            let payload = BookBookmarksFile {
                book_uuid: local_book.uuid.clone(),
                updated_at: Utc::now().timestamp_millis(),
                bookmarks,
            };
            if let Ok(json_bytes) = serde_json::to_vec(&payload) {
                let _ = client.upload_bytes(&format!("/AudireReader/bookmarks/{}.json", local_book.uuid), json_bytes).await;
            }
        }

        if let Ok(highlights) = crate::api::database::get_highlights(local_book.uuid.clone()) {
            let payload = BookHighlightsFile {
                book_uuid: local_book.uuid.clone(),
                updated_at: Utc::now().timestamp_millis(),
                highlights,
            };
            if let Ok(json_bytes) = serde_json::to_vec(&payload) {
                let _ = client.upload_bytes(&format!("/AudireReader/highlights/{}.json", local_book.uuid), json_bytes).await;
            }
        }
    }

    let _ = crate::api::database::insert_sync_history(SyncHistoryEntry {
        id: None,
        timestamp: Utc::now().timestamp_millis(),
        action: if progress_only { "force_push_progress".to_string() } else { "force_push_all".to_string() },
        status: "success".to_string(),
        details: format!("Force pushed {} books to cloud", local_books.len()),
    });

    record_sync_success_timestamp();

    Ok(SyncResult {
        success: true,
        message: "Force push completed successfully".to_string(),
        local_changed: false,
    })
}

pub async fn force_pull(progress_only: bool, documents_dir: String) -> Result<SyncResult, String> {
    let client = get_or_init_client().await?;

    if !progress_only {
        let cloud_books = fetch_cloud_books(Some(documents_dir.clone())).await?;
        for cb in cloud_books {
            let _ = download_and_import_book(cb.uuid, documents_dir.clone()).await;
        }
    }

    let local_books = crate::api::database::get_all_books()?;
    for local_book in &local_books {
        let remote_path = format!("/AudireReader/progress/{}.json", local_book.uuid);
        if let Ok(bytes) = client.download_bytes(&remote_path).await {
            if let Ok(cloud_prog) = serde_json::from_slice::<SyncProgressPayload>(&bytes) {
                let prog = ReadingProgress {
                    id: None,
                    book_uuid: local_book.uuid.clone(),
                    current_chapter_index: cloud_prog.chapter_index,
                    current_paragraph_index: cloud_prog.paragraph_index,
                    current_character_offset: cloud_prog.character_offset,
                    last_read: cloud_prog.last_read,
                };
                let _ = crate::api::database::save_reading_progress(prog);
            }
        }

        // Pull cloud bookmarks & highlights
        let remote_bm = format!("/AudireReader/bookmarks/{}.json", local_book.uuid);
        if let Ok(bytes) = client.download_bytes(&remote_bm).await {
            if let Ok(bm_file) = serde_json::from_slice::<BookBookmarksFile>(&bytes) {
                let _ = crate::api::database::replace_all_bookmarks(local_book.uuid.clone(), bm_file.bookmarks);
            }
        }

        let remote_hl = format!("/AudireReader/highlights/{}.json", local_book.uuid);
        if let Ok(bytes) = client.download_bytes(&remote_hl).await {
            if let Ok(hl_file) = serde_json::from_slice::<BookHighlightsFile>(&bytes) {
                let _ = crate::api::database::replace_all_highlights(local_book.uuid.clone(), hl_file.highlights);
            }
        }
    }

    let _ = crate::api::database::insert_sync_history(SyncHistoryEntry {
        id: None,
        timestamp: Utc::now().timestamp_millis(),
        action: if progress_only { "force_pull_progress".to_string() } else { "force_pull_all".to_string() },
        status: "success".to_string(),
        details: "Force pulled cloud data to local".to_string(),
    });

    record_sync_success_timestamp();

    Ok(SyncResult {
        success: true,
        message: "Force pull completed successfully".to_string(),
        local_changed: true,
    })
}

pub async fn force_push_book(book_uuid: String) -> Result<SyncResult, String> {
    let ok = export_and_upload_book(book_uuid.clone()).await?;
    let _ = sync_book_progress(book_uuid.clone()).await;
    let _ = sync_book_bookmarks(book_uuid.clone()).await;
    let _ = sync_book_highlights(book_uuid.clone()).await;

    // Ensure sync_data.json includes this book
    if let Ok(client) = get_or_init_client().await {
        if let Ok(Some(local_book)) = crate::api::database::get_book_by_uuid(book_uuid.clone()) {
            let mut sync_data = if client.file_exists("/AudireReader/sync_data.json").await.unwrap_or(false) {
                if let Ok(bytes) = client.download_bytes("/AudireReader/sync_data.json").await {
                    serde_json::from_slice::<SyncDataFile>(&bytes).unwrap_or_else(|_| SyncDataFile {
                        version: 1,
                        last_sync_time: Utc::now().to_rfc3339(),
                        books: vec![],
                        deleted: vec![],
                    })
                } else {
                    SyncDataFile {
                        version: 1,
                        last_sync_time: Utc::now().to_rfc3339(),
                        books: vec![],
                        deleted: vec![],
                    }
                }
            } else {
                SyncDataFile {
                    version: 1,
                    last_sync_time: Utc::now().to_rfc3339(),
                    books: vec![],
                    deleted: vec![],
                }
            };

            if !sync_data.books.iter().any(|b| b.uuid == book_uuid) {
                let ext = local_book.cover_path.as_ref().and_then(|p| Path::new(p).extension().and_then(|e| e.to_str()).map(|e| format!(".{}", e)));
                sync_data.books.push(SyncDataBookItem {
                    uuid: local_book.uuid.clone(),
                    title: local_book.title.clone(),
                    author: local_book.author.clone(),
                    total_chapters: local_book.total_chapters,
                    date_added: chrono::DateTime::from_timestamp_millis(local_book.date_added).map(|dt| dt.to_rfc3339()).unwrap_or_else(|| Utc::now().to_rfc3339()),
                    cover_extension: ext,
                    has_cover: local_book.cover_path.is_some(),
                });
                sync_data.last_sync_time = Utc::now().to_rfc3339();
                if let Ok(sync_bytes) = serde_json::to_vec(&sync_data) {
                    let _ = client.upload_bytes("/AudireReader/sync_data.json", sync_bytes).await;
                }
            }
        }
    }

    if ok {
        record_sync_success_timestamp();
    }

    Ok(SyncResult {
        success: ok,
        message: format!("Pushed book {} to cloud", book_uuid),
        local_changed: false,
    })
}

pub async fn force_pull_book(book_uuid: String, documents_dir: String) -> Result<SyncResult, String> {
    let _ = download_and_import_book(book_uuid.clone(), documents_dir).await?;
    let _ = sync_book_progress(book_uuid.clone()).await;
    let _ = sync_book_bookmarks(book_uuid.clone()).await;
    let _ = sync_book_highlights(book_uuid.clone()).await;
    record_sync_success_timestamp();
    Ok(SyncResult {
        success: true,
        message: format!("Pulled book {} from cloud", book_uuid),
        local_changed: true,
    })
}

pub async fn delete_book_from_cloud(book_uuid: String) -> Result<SyncResult, String> {
    let client = get_or_init_client().await?;

    let remote_book = format!("/AudireReader/books/{}.json.gz", book_uuid);
    let _ = client.remove(&remote_book).await;

    let remote_prog = format!("/AudireReader/progress/{}.json", book_uuid);
    let _ = client.remove(&remote_prog).await;

    let remote_bm = format!("/AudireReader/bookmarks/{}.json", book_uuid);
    let _ = client.remove(&remote_bm).await;

    let remote_hl = format!("/AudireReader/highlights/{}.json", book_uuid);
    let _ = client.remove(&remote_hl).await;

    let _ = client.remove(&format!("/AudireReader/covers/{}.jpg", book_uuid)).await;
    let _ = client.remove(&format!("/AudireReader/covers/{}.png", book_uuid)).await;
    let _ = client.remove(&format!("/AudireReader/covers/{}.jpeg", book_uuid)).await;

    if client.file_exists("/AudireReader/sync_data.json").await.unwrap_or(false) {
        if let Ok(bytes) = client.download_bytes("/AudireReader/sync_data.json").await {
            if let Ok(mut sync_data) = serde_json::from_slice::<SyncDataFile>(&bytes) {
                sync_data.books.retain(|b| b.uuid != book_uuid);
                sync_data.deleted.push(SyncDeletedItem::Detailed {
                    uuid: book_uuid.clone(),
                    deleted_at: Utc::now().to_rfc3339(),
                });
                sync_data.last_sync_time = Utc::now().to_rfc3339();
                if let Ok(new_bytes) = serde_json::to_vec(&sync_data) {
                    let _ = client.upload_bytes("/AudireReader/sync_data.json", new_bytes).await;
                }
            }
        }
    }

    Ok(SyncResult {
        success: true,
        message: format!("Deleted book {} from cloud", book_uuid),
        local_changed: false,
    })
}

pub async fn upload_single_book(book_uuid: String) -> Result<SyncResult, String> {
    force_push_book(book_uuid).await
}

pub async fn download_virtual_book(book_uuid: String, documents_dir: String) -> Result<SyncResult, String> {
    let book = download_and_import_book(book_uuid.clone(), documents_dir).await?;
    let _ = sync_book_progress(book.uuid.clone()).await;
    record_sync_success_timestamp();
    Ok(SyncResult {
        success: true,
        message: format!("Downloaded virtual book {}", book.title),
        local_changed: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_optional_live_webdav() {
        let url = std::env::var("WEBDAV_TEST_URL").unwrap_or_default();
        let user = std::env::var("WEBDAV_TEST_USER").unwrap_or_default();
        let pass = std::env::var("WEBDAV_TEST_PASS").unwrap_or_default();

        if url.is_empty() || user.is_empty() {
            return; // Skip if environment variables are not provided
        }

        let client = WebDavClient::new(&url, &user, &pass).expect("Failed to create WebDavClient");
        let connected = client.test_connection().await;
        assert!(connected.unwrap_or(false), "Failed to connect to WebDAV");
    }

    #[test]
    fn test_webdav_client_url_normalization() {
        let c1 = WebDavClient::new("app.koofr.net/dav/Koofr", "user", "pass").unwrap();
        assert_eq!(c1.base_url, "https://app.koofr.net/dav/Koofr");

        let c2 = WebDavClient::new("http://my-nas.local:5005/webdav/", "user", "pass").unwrap();
        assert_eq!(c2.base_url, "http://my-nas.local:5005/webdav/");
    }

    #[test]
    fn test_webdav_xml_parsing() {
        let sample_xml = r#"<?xml version="1.0" encoding="utf-8"?>
        <d:multistatus xmlns:d="DAV:">
            <d:response>
                <d:href>/dav/Koofr/AudireReader/books/</d:href>
                <d:propstat>
                    <d:prop>
                        <d:displayname>books</d:displayname>
                        <d:resourcetype><d:collection/></d:resourcetype>
                        <d:getcontentlength>0</d:getcontentlength>
                    </d:prop>
                    <d:status>HTTP/1.1 200 OK</d:status>
                </d:propstat>
            </d:response>
            <d:response>
                <d:href>/dav/Koofr/AudireReader/books/my_book.json.gz</d:href>
                <d:propstat>
                    <d:prop>
                        <d:displayname>my_book.json.gz</d:displayname>
                        <d:resourcetype/>
                        <d:getcontentlength>123456</d:getcontentlength>
                        <d:getlastmodified>Wed, 19 Aug 2026 06:51:09 GMT</d:getlastmodified>
                    </d:prop>
                    <d:status>HTTP/1.1 200 OK</d:status>
                </d:propstat>
            </d:response>
        </d:multistatus>"#;

        let files = parse_webdav_xml(sample_xml).expect("Should parse XML");
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].name, "books");
        assert_eq!(files[0].is_dir, true);
        assert_eq!(files[1].name, "my_book.json.gz");
        assert_eq!(files[1].is_dir, false);
        assert_eq!(files[1].size, 123456);
    }

    #[test]
    fn test_gzip_roundtrip() {
        let original_data = b"Hello, AudireReader WebDAV synchronization test!".repeat(100);
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&original_data).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.starts_with(&[0x1f, 0x8b]));

        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut decompressed).unwrap();
        assert_eq!(decompressed, original_data);
    }

    #[test]
    fn test_webdav_password_storage() {
        let temp_dir = std::env::temp_dir().join("audire_reader_sync_test");
        let _ = std::fs::create_dir_all(&temp_dir);
        let _ = crate::api::database::init_database(temp_dir.to_string_lossy().to_string());

        let set_res = save_webdav_password("secret_pass_123".to_string());
        assert!(set_res.is_ok());

        let loaded = get_webdav_password();
        assert_eq!(loaded.unwrap(), Some("secret_pass_123".to_string()));

        let del_res = delete_webdav_password();
        assert!(del_res.is_ok());

        let after_delete = get_webdav_password();
        assert_eq!(after_delete.unwrap(), None);
    }
}

