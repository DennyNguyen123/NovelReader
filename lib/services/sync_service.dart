import 'dart:async';
import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:audire_reader/src/rust/api/models.dart';
import 'package:audire_reader/src/rust/api/sync.dart' as rust_sync;
import 'package:audire_reader/src/rust/api/database.dart' as rust_db;
import '../core/utils/path_helper.dart';
import 'logger_service.dart';

class SyncService {
  static SyncService? _instance;
  bool _isSyncing = false;
  StreamSubscription<SyncProgressEvent>? _eventSub;

  final ValueNotifier<Map<String, String>> syncStateNotifier = ValueNotifier({});
  final ValueNotifier<List<Map<String, dynamic>>> cloudBooksNotifier = ValueNotifier([]);
  final ValueNotifier<Set<String>> cloudBookUuidsNotifier = ValueNotifier({});

  SyncService._() {
    _initEventListener();
  }

  static SyncService getInstance() {
    _instance ??= SyncService._();
    return _instance!;
  }

  bool get isSyncing => _isSyncing;
  StreamSubscription<SyncProgressEvent>? get eventSubscription => _eventSub;

  void dispose() {
    _eventSub?.cancel();
    _eventSub = null;
  }

  void _initEventListener() {
    try {
      _eventSub = rust_sync.subscribeSyncEvents().listen((event) {
        if (event.bookUuid != null && event.status != null) {
          _updateBookSyncStatus(event.bookUuid!, event.status!);
        }
        if (event.eventType == 'syncFinished') {
          _isSyncing = false;
        }
      }, onError: (e) {
        LoggerService().log('Sync stream error: $e', tag: 'SYNC', level: LogLevel.error);
      });
    } catch (e) {
      LoggerService().log('Failed to subscribe sync events: $e', tag: 'SYNC', level: LogLevel.warning);
    }
  }

  void _updateBookSyncStatus(String bookUuid, String status) {
    final newMap = Map<String, String>.from(syncStateNotifier.value);
    newMap[bookUuid] = status;
    syncStateNotifier.value = newMap;

    if (status == 'success' || status == 'error') {
      Future.delayed(const Duration(seconds: 3), () {
        final currentMap = Map<String, String>.from(syncStateNotifier.value);
        if (currentMap[bookUuid] == status) {
          currentMap.remove(bookUuid);
          syncStateNotifier.value = currentMap;
        }
      });
    }
  }

  Future<void> fetchCloudBooks() async {
    try {
      final docDir = (await PathHelper.getAppDirectory()).path;
      final books = await rust_sync.fetchCloudBooks(documentsDir: docDir);
      final parsed = books.map((b) => {
        'uuid': b.uuid,
        'title': b.title,
        'author': b.author,
        'totalChapters': b.totalChapters,
        'coverExtension': b.coverExtension,
        'hasCover': b.hasCover,
        'dateAdded': b.dateAdded,
      }).toList();

      cloudBooksNotifier.value = parsed;
      cloudBookUuidsNotifier.value = parsed.map((b) => b['uuid'] as String).toSet();
    } catch (e) {
      LoggerService().log('Failed to fetch cloud books: $e', tag: 'SYNC', level: LogLevel.error);
    }
  }

  Future<SyncResult> sync() async {
    if (_isSyncing) {
      return const SyncResult(success: false, message: 'Sync already in progress.', localChanged: false);
    }
    _isSyncing = true;
    try {
      final docDir = (await PathHelper.getAppDirectory()).path;
      final result = await rust_sync.syncAll(documentsDir: docDir);
      await fetchCloudBooks();
      _isSyncing = false;
      return result;
    } catch (e) {
      _isSyncing = false;
      return SyncResult(success: false, message: 'Sync failed: $e', localChanged: false);
    }
  }

  Future<SyncResult> syncLibrary() => sync();

  Future<ProgressSyncResult> syncBookProgress(String bookUuid) async {
    try {
      return await rust_sync.syncBookProgress(bookUuid: bookUuid);
    } catch (e) {
      return ProgressSyncResult(status: 'error', message: e.toString());
    }
  }

  Future<bool> syncBookBookmarks(String bookUuid) async {
    try {
      return await rust_sync.syncBookBookmarks(bookUuid: bookUuid);
    } catch (_) {
      return false;
    }
  }

  Future<bool> syncBookHighlights(String bookUuid) async {
    try {
      return await rust_sync.syncBookHighlights(bookUuid: bookUuid);
    } catch (_) {
      return false;
    }
  }

  Future<void> forceUploadLocalProgress(String bookUuid) async {
    await syncBookProgress(bookUuid);
  }

  Future<void> forceUpdateLocalFromCloud(String bookUuid) async {
    await syncBookProgress(bookUuid);
  }

  Future<SyncResult> forcePush({bool progressOnly = true}) async {
    _isSyncing = true;
    try {
      final result = await rust_sync.forcePush(progressOnly: progressOnly);
      await fetchCloudBooks();
      _isSyncing = false;
      return result;
    } catch (e) {
      _isSyncing = false;
      return SyncResult(success: false, message: 'Force push failed: $e', localChanged: false);
    }
  }

  Future<SyncResult> forcePull({bool progressOnly = true}) async {
    _isSyncing = true;
    try {
      final docDir = (await PathHelper.getAppDirectory()).path;
      final result = await rust_sync.forcePull(progressOnly: progressOnly, documentsDir: docDir);
      await fetchCloudBooks();
      _isSyncing = false;
      return result;
    } catch (e) {
      _isSyncing = false;
      return SyncResult(success: false, message: 'Force pull failed: $e', localChanged: false);
    }
  }

  Future<SyncResult> forcePushBook(String bookUuid) async {
    _updateBookSyncStatus(bookUuid, 'syncing');
    try {
      final result = await rust_sync.forcePushBook(bookUuid: bookUuid);
      _updateBookSyncStatus(bookUuid, result.success ? 'success' : 'error');
      await fetchCloudBooks();
      return result;
    } catch (e) {
      _updateBookSyncStatus(bookUuid, 'error');
      return SyncResult(success: false, message: 'Push failed: $e', localChanged: false);
    }
  }

  Future<SyncResult> forcePullBook(String bookUuid) async {
    _updateBookSyncStatus(bookUuid, 'syncing');
    try {
      final docDir = (await PathHelper.getAppDirectory()).path;
      final result = await rust_sync.forcePullBook(bookUuid: bookUuid, documentsDir: docDir);
      _updateBookSyncStatus(bookUuid, result.success ? 'success' : 'error');
      return result;
    } catch (e) {
      _updateBookSyncStatus(bookUuid, 'error');
      return SyncResult(success: false, message: 'Pull failed: $e', localChanged: false);
    }
  }

  Future<SyncResult> deleteBookFromCloud(String bookUuid) async {
    try {
      final result = await rust_sync.deleteBookFromCloud(bookUuid: bookUuid);
      await fetchCloudBooks();
      return result;
    } catch (e) {
      return SyncResult(success: false, message: 'Delete failed: $e', localChanged: false);
    }
  }

  Future<SyncResult> uploadSingleBook(String bookUuid) async {
    _updateBookSyncStatus(bookUuid, 'syncing');
    try {
      final result = await rust_sync.uploadSingleBook(bookUuid: bookUuid);
      _updateBookSyncStatus(bookUuid, result.success ? 'success' : 'error');
      await fetchCloudBooks();
      return result;
    } catch (e) {
      _updateBookSyncStatus(bookUuid, 'error');
      return SyncResult(success: false, message: 'Upload failed: $e', localChanged: false);
    }
  }

  Future<SyncResult> downloadVirtualBook(String bookUuid) async {
    _updateBookSyncStatus(bookUuid, 'syncing');
    try {
      final docDir = (await PathHelper.getAppDirectory()).path;
      final result = await rust_sync.downloadVirtualBook(bookUuid: bookUuid, documentsDir: docDir);
      _updateBookSyncStatus(bookUuid, result.success ? 'success' : 'error');
      return result;
    } catch (e) {
      _updateBookSyncStatus(bookUuid, 'error');
      return SyncResult(success: false, message: 'Download failed: $e', localChanged: false);
    }
  }

  Future<List<Map<String, dynamic>>> getSyncHistory() async {
    try {
      final history = await rust_db.getSyncHistory();
      return history.map((h) => {
        'id': h.id,
        'timestamp': h.timestamp,
        'action': h.action,
        'status': h.status,
        'details': h.details,
      }).toList();
    } catch (e) {
      return [];
    }
  }
}
