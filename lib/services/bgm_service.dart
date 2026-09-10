import 'dart:async';
import 'dart:io';
import 'package:just_audio/just_audio.dart' as ja;
import 'package:flutter/foundation.dart';
import 'package:path/path.dart' as p;
import '../core/database/database_helper.dart';
import '../core/utils/path_helper.dart';
import 'package:audire_reader/src/rust/api/models.dart';
import 'logger_service.dart';
import 'bgm/bgm_provider.dart';
import 'bgm/local_bgm_provider.dart';
import 'bgm/radio_browser_provider.dart';
import 'bgm/open_lofi_provider.dart';

enum BgmState { idle, loading, playing, paused, error }

class BgmService extends ChangeNotifier {
  static BgmService? _instance;
  final ja.AudioPlayer _audioPlayer = ja.AudioPlayer();
  final Completer<void> _initCompleter = Completer<void>();

  bool _bgmEnabled = false;
  double _bgmVolume = 0.15;
  String _bgmLoopMode = 'all'; // 'none', 'one', 'all'
  String _bgmProviderId = 'local';
  final List<BgmProvider> _providers = [
    LocalBgmProvider(),
    RadioBrowserProvider(),
    OpenLofiProvider(),
  ];

  int? _currentBgmTrackId;
  List<BgmTrack> _bgmPlaylist = [];
  BgmTrack? _currentTrack;
  BgmState _state = BgmState.idle;
  int _bgmSessionId = 0;
  bool _isInit = false;
  bool _hasSource = false;

  bool get bgmEnabled => _bgmEnabled;
  double get bgmVolume => _bgmVolume;
  String get bgmLoopMode => _bgmLoopMode;
  String get bgmProviderId => _bgmProviderId;
  List<BgmProvider> get providers => _providers;
  int? get currentBgmTrackId => _currentBgmTrackId;
  List<BgmTrack> get bgmPlaylist => _bgmPlaylist;
  BgmTrack? get currentTrack => _currentTrack;
  bool get isPlaying => _state == BgmState.playing;
  bool get isLoading => _state == BgmState.loading;
  bool get isPaused => _state == BgmState.paused;
  BgmState get state => _state;

  BgmService._() {
    LoggerService().log("Constructor started", tag: 'BGM');
    _init();
  }

  static BgmService getInstance() {
    _instance ??= BgmService._();
    return _instance!;
  }

  Future<void> _init() async {
    LoggerService().log("_init started", tag: 'BGM');
    if (_isInit) return;

    // Lắng nghe sự kiện phát hết nhạc
    _audioPlayer.playerStateStream.listen((state) {
      if (state.processingState == ja.ProcessingState.completed) {
        _onTrackComplete();
      }
    });

    try {
      await loadSettingsAndPlaylist();
      _isInit = true;
      LoggerService().log("_init completed", tag: 'BGM');
    } catch (e) {
      LoggerService().log(
        "init error",
        tag: 'BGM',
        level: LogLevel.error,
        error: e.toString(),
      );
    } finally {
      if (!_initCompleter.isCompleted) {
        _initCompleter.complete();
      }
    }
  }

  Future<void> _loadPlaylistForCurrentProvider() async {
    try {
      final provider = _providers.firstWhere(
        (p) => p.id == _bgmProviderId,
        orElse: () => _providers.first,
      );
      _bgmPlaylist = await provider.fetchTracks();
    } catch (e) {
      LoggerService().log(
        "Fetch tracks error",
        tag: 'BGM',
        level: LogLevel.error,
        error: e.toString(),
      );
      _bgmPlaylist = [];
    }
  }

  Future<void> changeProvider(String providerId) async {
    if (_bgmProviderId == providerId) return;
    _bgmProviderId = providerId;
    await stopBgm();

    final db = await DatabaseHelper.getInstance();
    var settings = await db.getSettings();
    settings = settings.copyWith(bgmProviderId: providerId);
    await db.saveSettings(settings);

    await _loadPlaylistForCurrentProvider();

    // Khôi phục track lịch sử của provider mới
    String? targetUrl;
    String? targetName;
    if (providerId == 'local') {
      targetUrl = settings.lastLocalTrackUrl;
    } else if (providerId == 'radio_browser') {
      targetUrl = settings.lastRadioTrackUrl;
      targetName = settings.lastRadioTrackName;
    } else if (providerId == 'open_lofi') {
      targetUrl = settings.lastLofiTrackUrl;
      targetName = settings.lastLofiTrackName;
    }

    BgmTrack? matchedTrack;
    if (_bgmPlaylist.isNotEmpty) {
      if (targetUrl != null) {
        final matches = _bgmPlaylist.where((t) => t.sourcePath == targetUrl);
        if (matches.isNotEmpty) {
          matchedTrack = matches.first;
        }
      }
      if (matchedTrack == null &&
          settings.currentBgmTrackId != null &&
          providerId == 'local') {
        final matches = _bgmPlaylist.where(
          (t) => t.id == settings.currentBgmTrackId,
        );
        if (matches.isNotEmpty) {
          matchedTrack = matches.first;
        }
      }
    }

    if (matchedTrack != null) {
      _currentTrack = matchedTrack;
      _currentBgmTrackId = matchedTrack.id;
    } else if (targetUrl != null && targetUrl.startsWith('http')) {
      final tempTrack = BgmTrack(
        name: targetName ?? 'Last Station',
        sourceType: providerId == 'radio_browser' ? 'radio' : 'openlofi',
        sourcePath: targetUrl,
        dateAdded: DateTime.now().millisecondsSinceEpoch,
      );
      _currentTrack = tempTrack;
      _currentBgmTrackId = tempTrack.id;
    } else if (_bgmPlaylist.isNotEmpty) {
      _currentTrack = _bgmPlaylist.first;
      _currentBgmTrackId = _currentTrack?.id;
    } else {
      _currentTrack = null;
      _currentBgmTrackId = null;
    }

    // Cập nhật lại cấu hình hiện tại trong database
    final updatedSettings = settings.copyWith(
      currentBgmTrackId: _currentBgmTrackId,
      currentBgmTrackUrl: _currentTrack?.sourcePath,
      currentBgmTrackName: _currentTrack?.name,
    );
    await db.saveSettings(updatedSettings);

    notifyListeners();
  }

  Future<void> loadSettingsAndPlaylist() async {
    try {
      LoggerService().log(
        "loadSettingsAndPlaylist started, getting DatabaseHelper",
        tag: 'BGM',
      );
      final db = await DatabaseHelper.getInstance();

      // Load settings
      LoggerService().log(
        "loadSettingsAndPlaylist, getting settings",
        tag: 'BGM',
      );
      var settings = await db.getSettings();

      // Sửa lỗi deserialize trường double mới thành NaN trên bản ghi cũ
      if (settings.bgmVolume.isNaN ||
          settings.bgmVolume < 0.0 ||
          settings.bgmVolume > 1.0) {
        settings = settings.copyWith(bgmVolume: 0.15);
        await db.saveSettings(settings);
      }

      _bgmEnabled = settings.bgmEnabled;
      _bgmVolume = settings.bgmVolume;

      _bgmProviderId = 'local';
      if (settings.bgmProviderId != 'local') {
        settings = settings.copyWith(bgmProviderId: 'local');
        await db.saveSettings(settings);
      }
      // Load playlist depending on provider
      LoggerService().log(
        "loadSettingsAndPlaylist, loading tracks for provider $_bgmProviderId",
        tag: 'BGM',
      );
      await _loadPlaylistForCurrentProvider();

      dynamic rawLoopMode = settings.bgmLoopMode;
      _bgmLoopMode = (rawLoopMode == null || rawLoopMode.toString().isEmpty)
          ? 'all'
          : rawLoopMode.toString();
      if (rawLoopMode == null) {
        settings = settings.copyWith(bgmLoopMode: _bgmLoopMode);
        await db.saveSettings(settings);
      }

      _currentBgmTrackId = settings.currentBgmTrackId;

      LoggerService().log(
        "loadSettingsAndPlaylist, setting audio volume to $_bgmVolume",
        tag: 'BGM',
      );
      try {
        if (!_bgmVolume.isNaN) {
          await _audioPlayer.setVolume(_bgmVolume);
          LoggerService().log(
            "loadSettingsAndPlaylist, setVolume completed",
            tag: 'BGM',
          );
        } else {
          _bgmVolume = 0.15;
          await _audioPlayer.setVolume(0.15);
        }
      } catch (volErr) {
        LoggerService().log(
          "loadSettingsAndPlaylist, setVolume error",
          tag: 'BGM',
          level: LogLevel.error,
          error: volErr.toString(),
        );
      }

      // Thiết lập currentTrack
      String? targetUrl;
      String? targetName;
      if (_bgmProviderId == 'local') {
        targetUrl = settings.lastLocalTrackUrl;
      } else if (_bgmProviderId == 'radio_browser') {
        targetUrl = settings.lastRadioTrackUrl;
        targetName = settings.lastRadioTrackName;
      } else if (_bgmProviderId == 'open_lofi') {
        targetUrl = settings.lastLofiTrackUrl;
        targetName = settings.lastLofiTrackName;
      }

      BgmTrack? matchedTrack;
      if (_bgmPlaylist.isNotEmpty) {
        if (targetUrl != null) {
          final urlMatches = _bgmPlaylist.where(
            (t) => t.sourcePath == targetUrl,
          );
          if (urlMatches.isNotEmpty) {
            matchedTrack = urlMatches.first;
          }
        }

        if (matchedTrack == null && _currentBgmTrackId != null) {
          final idMatches = _bgmPlaylist.where(
            (t) => t.id == _currentBgmTrackId,
          );
          if (idMatches.isNotEmpty) {
            matchedTrack = idMatches.first;
          }
        }
      }

      if (matchedTrack != null) {
        _currentTrack = matchedTrack;
        _currentBgmTrackId = matchedTrack.id;
      } else if (targetUrl != null && targetUrl.startsWith('http')) {
        // Tự tạo track tạm thời cho nguồn internet nếu không tìm thấy trong danh sách tải về
        final tempTrack = BgmTrack(
          name: targetName ?? 'Last Station',
          sourceType: _bgmProviderId == 'radio_browser' ? 'radio' : 'openlofi',
          sourcePath: targetUrl,
          dateAdded: DateTime.now().millisecondsSinceEpoch,
        );
        _currentTrack = tempTrack;
        _currentBgmTrackId = tempTrack.id;
      } else if (_bgmPlaylist.isNotEmpty) {
        _currentTrack = _bgmPlaylist.first;
        _currentBgmTrackId = _currentTrack?.id;
      } else {
        _currentTrack = null;
        _currentBgmTrackId = null;
      }

      LoggerService().log(
        "loadSettingsAndPlaylist, calling notifyListeners",
        tag: 'BGM',
      );
      notifyListeners();
      LoggerService().log(
        "loadSettingsAndPlaylist completed successfully",
        tag: 'BGM',
      );
    } catch (e) {
      LoggerService().log(
        "init error",
        tag: 'BGM',
        level: LogLevel.error,
        error: e.toString(),
      );
    }
  }

  Future<void> updateSettings({
    bool? bgmEnabled,
    double? bgmVolume,
    String? bgmLoopMode,
    int? currentBgmTrackId,
    String? currentBgmTrackUrl,
    String? currentBgmTrackName,
  }) async {
    final db = await DatabaseHelper.getInstance();
    final settings = await db.getSettings();

    if (bgmEnabled != null) {
      _bgmEnabled = bgmEnabled;
      if (!bgmEnabled) {
        await stopBgm();
      }
    }
    if (bgmVolume != null) {
      _bgmVolume = bgmVolume;
      await _audioPlayer.setVolume(bgmVolume);
    }
    if (bgmLoopMode != null) {
      _bgmLoopMode = bgmLoopMode;
    }
    if (currentBgmTrackId != null) {
      _currentBgmTrackId = currentBgmTrackId;
      final matches = _bgmPlaylist.where((t) => t.id == currentBgmTrackId);
      if (matches.isNotEmpty) {
        _currentTrack = matches.first;
      }
    }

    String? lastLocal = settings.lastLocalTrackUrl;
    String? lastRadioUrl = settings.lastRadioTrackUrl;
    String? lastRadioName = settings.lastRadioTrackName;
    String? lastLofiUrl = settings.lastLofiTrackUrl;
    String? lastLofiName = settings.lastLofiTrackName;

    if (currentBgmTrackUrl != null) {
      if (_bgmProviderId == 'local') {
        lastLocal = currentBgmTrackUrl;
      } else if (_bgmProviderId == 'radio_browser') {
        lastRadioUrl = currentBgmTrackUrl;
        if (currentBgmTrackName != null) {
          lastRadioName = currentBgmTrackName;
        }
      } else if (_bgmProviderId == 'open_lofi') {
        lastLofiUrl = currentBgmTrackUrl;
        if (currentBgmTrackName != null) {
          lastLofiName = currentBgmTrackName;
        }
      }
    }

    final updated = settings.copyWith(
      bgmEnabled: bgmEnabled ?? settings.bgmEnabled,
      bgmVolume: bgmVolume ?? settings.bgmVolume,
      bgmLoopMode: bgmLoopMode ?? settings.bgmLoopMode,
      currentBgmTrackId: currentBgmTrackId ?? settings.currentBgmTrackId,
      currentBgmTrackUrl: currentBgmTrackUrl ?? settings.currentBgmTrackUrl,
      currentBgmTrackName: currentBgmTrackName ?? settings.currentBgmTrackName,
      lastLocalTrackUrl: lastLocal,
      lastRadioTrackUrl: lastRadioUrl,
      lastRadioTrackName: lastRadioName,
      lastLofiTrackUrl: lastLofiUrl,
      lastLofiTrackName: lastLofiName,
    );

    await db.saveSettings(updated);
    notifyListeners();
  }

  void updateVolumeInMemory(double volume) {
    if (volume.isNaN || volume < 0.0 || volume > 1.0) return;
    _bgmVolume = volume;
    _audioPlayer.setVolume(volume).catchError((_) {});
    notifyListeners();
  }

  // --- Phát nhạc ---
  Future<void> playTrack(BgmTrack track) async {
    final sessionId = ++_bgmSessionId;
    await _audioPlayer.stop();
    _currentTrack = track;

    _currentBgmTrackId = track.id;
    await updateSettings(
      currentBgmTrackId: track.id,
      currentBgmTrackUrl: track.sourcePath,
      currentBgmTrackName: track.name,
    );

    if (!_bgmEnabled) {
      _state = BgmState.idle;
      notifyListeners();
      return;
    }

    try {
      _state = BgmState.loading;
      notifyListeners();

      if (track.sourceType == 'local') {
        final appDir = await PathHelper.getAppDirectory();
        final fileName = p.basename(track.sourcePath);
        final file = File(p.join(appDir.path, 'bgm', fileName));
        if (await file.exists()) {
          if (sessionId != _bgmSessionId || !_bgmEnabled || _state != BgmState.loading) {
            await _audioPlayer.stop();
            return;
          }
          await _audioPlayer.setFilePath(file.path);
          if (sessionId != _bgmSessionId || !_bgmEnabled || _state != BgmState.loading) {
            await _audioPlayer.stop();
            return;
          }
          await _audioPlayer.play();
          _hasSource = true;
        } else {
          throw Exception("Local BGM file does not exist: ${file.path}");
        }
      } else if (track.sourceType == 'radio' ||
          track.sourceType == 'openlofi' ||
          track.sourceType == 'direct_url') {
        // Stream from internet
        if (sessionId != _bgmSessionId || !_bgmEnabled || _state != BgmState.loading) {
          await _audioPlayer.stop();
          return;
        }
        await _audioPlayer.setUrl(track.sourcePath);
        if (sessionId != _bgmSessionId || !_bgmEnabled || _state != BgmState.loading) {
          await _audioPlayer.stop();
          return;
        }
        await _audioPlayer.play();
        _hasSource = true;
      } else {
        throw Exception("Unsupported BGM source type: ${track.sourceType}");
      }

      if (sessionId != _bgmSessionId || !_bgmEnabled || _state != BgmState.loading) {
        await _audioPlayer.stop();
        return;
      }

      await _audioPlayer.setVolume(_bgmVolume);
      _state = BgmState.playing;
      notifyListeners();
    } catch (e) {
      if (sessionId != _bgmSessionId) return;
      LoggerService().log(
        "Error playing BGM",
        tag: 'BGM',
        level: LogLevel.error,
        error: e.toString(),
      );
      _state = BgmState.error;
      _hasSource = false;
      notifyListeners();

      // Tự động Fallback sang Local nếu đang dùng Internet Provider bị lỗi
      if (_bgmProviderId != 'local' && _bgmEnabled) {
        LoggerService().log(
          "Network BGM failed, falling back to local provider...",
          tag: 'BGM',
        );
        await changeProvider('local');
        if (_currentTrack != null) {
          await playTrack(_currentTrack!);
        }
      }
    }
  }

  Future<void> resumeBgm() async {
    // Chờ quá trình khởi nạp settings từ database hoàn tất
    if (!_isInit) {
      await _initCompleter.future;
    }

    if (!_bgmEnabled || _state == BgmState.playing || _state == BgmState.loading) return;

    if (_currentTrack != null) {
      if (_currentTrack!.sourceType == 'local') {
        final appDir = await PathHelper.getAppDirectory();
        final fileName = p.basename(_currentTrack!.sourcePath);
        final file = File(p.join(appDir.path, 'bgm', fileName));
        if (await file.exists()) {
          if (_hasSource) {
            final sessionId = ++_bgmSessionId;
            _state = BgmState.playing;
            notifyListeners();
            await _audioPlayer.play();
            if (sessionId != _bgmSessionId || !_bgmEnabled || _state != BgmState.playing) {
              await _audioPlayer.pause();
              return;
            }
          } else {
            // Lần đầu tiên phát nhạc nền, nạp nguồn âm thanh từ đầu
            await playTrack(_currentTrack!);
          }
        } else {
          await playTrack(_currentTrack!);
        }
      } else {
        await playTrack(_currentTrack!);
      }
    } else if (_bgmPlaylist.isNotEmpty) {
      await playTrack(_bgmPlaylist.first);
    }
  }

  Future<void> pauseBgm() async {
    _bgmSessionId++;
    if (_state == BgmState.idle || _state == BgmState.paused) return;
    _state = BgmState.paused;
    await _audioPlayer.pause();
    notifyListeners();
  }

  Future<void> stopBgm() async {
    _bgmSessionId++;
    _state = BgmState.idle;
    _hasSource = false; // Reset nguồn khi dừng hẳn nhạc
    await _audioPlayer.stop();
    notifyListeners();
  }

  Future<void> nextTrack() async {
    if (_bgmPlaylist.isEmpty) return;
    if (_currentTrack == null) {
      await playTrack(_bgmPlaylist.first);
      return;
    }

    final currentIndex = _bgmPlaylist.indexWhere(
      (t) => t.id == _currentTrack!.id,
    );
    if (currentIndex == -1 || currentIndex >= _bgmPlaylist.length - 1) {
      await playTrack(_bgmPlaylist.first);
    } else {
      await playTrack(_bgmPlaylist[currentIndex + 1]);
    }
  }

  Future<void> previousTrack() async {
    if (_bgmPlaylist.isEmpty) return;
    if (_currentTrack == null) {
      await playTrack(_bgmPlaylist.first);
      return;
    }

    final currentIndex = _bgmPlaylist.indexWhere(
      (t) => t.id == _currentTrack!.id,
    );
    if (currentIndex == -1 || currentIndex == 0) {
      await playTrack(_bgmPlaylist.last);
    } else {
      await playTrack(_bgmPlaylist[currentIndex - 1]);
    }
  }

  void _onTrackComplete() {
    if (_state != BgmState.playing) return;

    if (_bgmLoopMode == 'one') {
      if (_currentTrack != null) {
        playTrack(_currentTrack!);
      }
    } else if (_bgmLoopMode == 'all') {
      nextTrack();
    } else {
      stopBgm();
    }
  }

  // --- Quản lý dữ liệu bài hát ---
  Future<void> addTrackFromLocal(String name, String originalFilePath) async {
    try {
      final appDir = await PathHelper.getAppDirectory();
      final bgmDir = Directory(p.join(appDir.path, 'bgm'));
      if (!bgmDir.existsSync()) {
        bgmDir.createSync(recursive: true);
      }

      final ext = p.extension(originalFilePath);
      final uniqueName = 'bgm_${DateTime.now().millisecondsSinceEpoch}$ext';
      final destPath = p.join(bgmDir.path, uniqueName);

      // Copy file vào thư mục ứng dụng
      final sourceFile = File(originalFilePath);
      if (await sourceFile.exists()) {
        await sourceFile.copy(destPath);
      } else {
        throw Exception("Source file does not exist: $originalFilePath");
      }

      final track = BgmTrack(
        name: name.trim().isEmpty
            ? p.basenameWithoutExtension(originalFilePath)
            : name,
        sourceType: 'local',
        sourcePath: destPath,
        dateAdded: DateTime.now().millisecondsSinceEpoch,
      );

      final db = await DatabaseHelper.getInstance();
      await db.saveBgmTrack(track);
      await loadSettingsAndPlaylist();
    } catch (e) {
      LoggerService().log(
        "Add local track error",
        tag: 'BGM',
        level: LogLevel.error,
        error: e.toString(),
      );
      rethrow;
    }
  }

  Future<void> deleteTrack(BgmTrack track) async {
    try {
      if (_currentTrack?.id == track.id) {
        await stopBgm();
      }

      // Xóa file vật lý nếu nguồn là file local
      if (track.sourceType == 'local') {
        final appDir = await PathHelper.getAppDirectory();
        final fileName = p.basename(track.sourcePath);
        final file = File(p.join(appDir.path, 'bgm', fileName));
        if (await file.exists()) {
          await file.delete();
        }
      }

      final db = await DatabaseHelper.getInstance();
      if (track.id != null) {
        await db.deleteBgmTrack(track.id!.toInt());
      }
      await loadSettingsAndPlaylist();
    } catch (e) {
      LoggerService().log(
        "Delete track error",
        tag: 'BGM',
        level: LogLevel.error,
        error: e.toString(),
      );
    }
  }

  Future<void> addTrackFromUrl(
    String name,
    String url, {
    String sourceType = 'direct_url',
  }) async {
    try {
      final track = BgmTrack(
        name: name.trim().isEmpty ? 'Direct Link' : name,
        sourceType: sourceType,
        sourcePath: url.trim(),
        dateAdded: DateTime.now().millisecondsSinceEpoch,
      );

      final db = await DatabaseHelper.getInstance();
      await db.saveBgmTrack(track);
      await loadSettingsAndPlaylist();
    } catch (e) {
      LoggerService().log(
        "Add track from URL error",
        tag: 'BGM',
        level: LogLevel.error,
        error: e.toString(),
      );
      rethrow;
    }
  }

  Future<void> updateTrack(
    BgmTrack track, {
    String? name,
    String? sourcePath,
  }) async {
    try {
      final db = await DatabaseHelper.getInstance();

      final updatedTrack = track.copyWith(
        name: (name != null && name.trim().isNotEmpty) ? name.trim() : track.name,
        sourcePath: (sourcePath != null &&
                sourcePath.trim().isNotEmpty &&
                track.sourceType == 'direct_url')
            ? sourcePath.trim()
            : track.sourcePath,
      );

      await db.saveBgmTrack(updatedTrack);

      if (_currentTrack?.id == track.id) {
        _currentTrack = updatedTrack;
        final settings = await db.getSettings();
        final updatedSettings = settings.copyWith(
          currentBgmTrackName: updatedTrack.name,
          currentBgmTrackUrl: updatedTrack.sourcePath,
          lastLocalTrackUrl: _bgmProviderId == 'local'
              ? updatedTrack.sourcePath
              : settings.lastLocalTrackUrl,
        );
        await db.saveSettings(updatedSettings);
      }

      await loadSettingsAndPlaylist();
    } catch (e) {
      LoggerService().log(
        "Update track error",
        tag: 'BGM',
        level: LogLevel.error,
        error: e.toString(),
      );
      rethrow;
    }
  }
}
