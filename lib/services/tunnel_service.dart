import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:dartssh2/dartssh2.dart';
import 'package:shelf/shelf.dart' as shelf;
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'logger_service.dart';

class ServerInfo {
  final int port;
  final String publicUrl;
  final List<String> localIps;

  ServerInfo({
    required this.port,
    required this.publicUrl,
    required this.localIps,
  });

  String get publicConfigUrl => '$publicUrl/config';
  String get primaryLanUrl {
    if (localIps.isNotEmpty) {
      return 'http://${localIps.first}:$port/config';
    }
    return 'http://127.0.0.1:$port/config';
  }
}

class TunnelService {
  HttpServer? _server;
  SSHClient? _sshClient;
  bool _isRunning = false;

  Function(Map<String, dynamic>)? onConfigReceived;

  // Key Ed25519 hợp lệ dùng để xác thực SSH Tunnel ẩn danh
  static const String _sshPrivateKeyPem = '''-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACCvtQ9hdHzD5GASkO1SoGQMkNFFs+VZGmlY5E7Y/lBVbAAAAJCq/uSrqv7k
qwAAAAtzc2gtZWQyNTUxOQAAACCvtQ9hdHzD5GASkO1SoGQMkNFFs+VZGmlY5E7Y/lBVbA
AAAEAgc0dk4OFB1cmOcwgm7VAz4TIZ2A0x5DPA/+nYkfE1cK+1D2F0fMPkYBKQ7VKgZAyQ
0UWz5VkaaVjkTtj+UFVsAAAAC2F1ZGlyZV9zeW5jAQI=
-----END OPENSSH PRIVATE KEY-----''';

  /// Khởi chạy server nhận cấu hình nội bộ và mở SSH Public Tunnel qua Internet
  Future<ServerInfo> startTunnel() async {
    await stopTunnel();

    // 1. Khởi chạy HTTP Server trên cổng ngẫu nhiên (lắng nghe mọi interface)
    final handler = const shelf.Pipeline()
        .addMiddleware(shelf.logRequests())
        .addHandler(_handleRequest);

    _server = await shelf_io.serve(handler, InternetAddress.anyIPv4, 0);
    final localPort = _server!.port;
    _isRunning = true;
    LoggerService().log(
      'Local HTTP Server running on port $localPort',
      tag: 'TUNNEL',
    );

    final localIps = await getLocalIpAddresses();

    // 2. Mở SSH Tunnel ra ngoài Internet (qua Pinggy port 443 hoặc fallback localhost.run)
    String? publicUrl;
    try {
      publicUrl = await _connectSshTunnel(localPort);
    } catch (e) {
      LoggerService().log(
        'Không thể mở SSH Public Tunnel: $e. Sử dụng Local IP fallback.',
        tag: 'TUNNEL',
        level: LogLevel.warning,
      );
    }

    final finalPublicUrl = publicUrl ?? (localIps.isNotEmpty ? 'http://${localIps.first}:$localPort' : 'http://127.0.0.1:$localPort');

    return ServerInfo(
      port: localPort,
      publicUrl: finalPublicUrl,
      localIps: localIps,
    );
  }

  Future<String> _connectSshTunnel(int localPort) async {
    final keys = SSHKeyPair.fromPem(_sshPrivateKeyPem);

    // Thử kết nối qua Pinggy (port 443 - hoạt động trên mọi mạng/firewall)
    try {
      LoggerService().log('Đang kết nối Cloud Tunnel (a.pinggy.io:443)...', tag: 'TUNNEL');
      final socket = await SSHSocket.connect(
        'a.pinggy.io',
        443,
        timeout: const Duration(seconds: 8),
      );

      _sshClient = SSHClient(
        socket,
        username: 'nokey',
        identities: keys,
      );

      await _sshClient!.authenticated;
      LoggerService().log('SSH Tunnel Authenticated!', tag: 'TUNNEL');

      final forward = await _sshClient!.forwardRemote(port: 80);
      if (forward != null) {
        forward.connections.listen((SSHForwardChannel channel) async {
          try {
            final localSocket = await Socket.connect('127.0.0.1', localPort);
            channel.stream.cast<List<int>>().pipe(localSocket);
            localSocket.cast<List<int>>().pipe(channel.sink);
          } catch (e) {
            LoggerService().log('Lỗi pipe tunnel: $e', tag: 'TUNNEL', level: LogLevel.error);
          }
        });
      }

      final session = await _sshClient!.shell(
        pty: const SSHPtyConfig(width: 80, height: 24),
      );

      final completer = Completer<String>();
      session.stdout.listen((Uint8List bytes) {
        final text = utf8.decode(bytes, allowMalformed: true);
        final regExp = RegExp(
          r'https://[a-zA-Z0-9-]+\.(?:free\.pinggy\.net|run\.pinggy-free\.link)',
        );
        final match = regExp.firstMatch(text);
        if (match != null && !completer.isCompleted) {
          completer.complete(match.group(0)!);
        }
      });

      return await completer.future.timeout(const Duration(seconds: 8));
    } catch (e) {
      LoggerService().log('Pinggy tunnel failed ($e), thử fallback localhost.run:22...', tag: 'TUNNEL');
      return await _connectLocalhostRun(localPort, keys);
    }
  }

  Future<String> _connectLocalhostRun(int localPort, List<SSHKeyPair> keys) async {
    final socket = await SSHSocket.connect(
      'localhost.run',
      22,
      timeout: const Duration(seconds: 8),
    );

    _sshClient = SSHClient(
      socket,
      username: 'nokey',
      identities: keys,
    );

    await _sshClient!.authenticated;
    final forward = await _sshClient!.forwardRemote(port: 80);
    if (forward != null) {
      forward.connections.listen((SSHForwardChannel channel) async {
        try {
          final localSocket = await Socket.connect('127.0.0.1', localPort);
          channel.stream.cast<List<int>>().pipe(localSocket);
          localSocket.cast<List<int>>().pipe(channel.sink);
        } catch (e) {
          LoggerService().log('Lỗi pipe localhost.run: $e', tag: 'TUNNEL', level: LogLevel.error);
        }
      });
    }

    final completer = Completer<String>();
    try {
      final session = await _sshClient!.execute('');
      session.stdout.listen((bytes) {
        final text = utf8.decode(bytes, allowMalformed: true);
        final regExp = RegExp(r'https://[a-zA-Z0-9-]+\.(?:lhr\.life|lhr\.pro)');
        final match = regExp.firstMatch(text);
        if (match != null && !completer.isCompleted) {
          completer.complete(match.group(0)!);
        }
      });
    } catch (_) {}

    return await completer.future.timeout(const Duration(seconds: 8));
  }

  /// Lấy danh sách địa chỉ IP mạng nội bộ
  static Future<List<String>> getLocalIpAddresses() async {
    final ips = <String>[];
    try {
      final interfaces = await NetworkInterface.list(
        type: InternetAddressType.IPv4,
      );
      for (final interface in interfaces) {
        for (final address in interface.addresses) {
          if (!address.isLoopback &&
              !address.address.startsWith('169.254.') &&
              !address.address.startsWith('127.')) {
            ips.add(address.address);
          }
        }
      }
      ips.sort((a, b) {
        final aScore = a.startsWith('192.168.') ? 0 : (a.startsWith('10.') ? 1 : 2);
        final bScore = b.startsWith('192.168.') ? 0 : (b.startsWith('10.') ? 1 : 2);
        return aScore.compareTo(bScore);
      });
    } catch (e) {
      LoggerService().log('Lỗi quét IP nội bộ: $e', tag: 'TUNNEL');
    }
    return ips;
  }

  Future<shelf.Response> _handleRequest(shelf.Request request) async {
    if (request.method == 'OPTIONS') {
      return shelf.Response.ok('', headers: _corsHeaders());
    }

    if (request.url.path == 'config') {
      if (request.method == 'POST') {
        try {
          final payload = await request.readAsString();
          final data = jsonDecode(payload) as Map<String, dynamic>;

          LoggerService().log('Đã nhận cấu hình WebDAV qua Tunnel!', tag: 'TUNNEL');

          if (onConfigReceived != null) {
            onConfigReceived!(data);
          }

          return shelf.Response.ok(
            jsonEncode({'success': true, 'status': 'ok', 'message': 'Config received successfully'}),
            headers: {'content-type': 'application/json', ..._corsHeaders()},
          );
        } catch (e) {
          LoggerService().log('Lỗi parse JSON cấu hình: $e', tag: 'TUNNEL', level: LogLevel.error);
          return shelf.Response.internalServerError(
            body: jsonEncode({'error': e.toString()}),
            headers: {'content-type': 'application/json', ..._corsHeaders()},
          );
        }
      } else if (request.method == 'GET') {
        return shelf.Response.ok(
          jsonEncode({'status': 'ready', 'service': 'Audire Sync Receiver'}),
          headers: {'content-type': 'application/json', ..._corsHeaders()},
        );
      }
    }

    return shelf.Response.ok('Audire Sync Server', headers: _corsHeaders());
  }

  Map<String, String> _corsHeaders() => {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Origin, Content-Type, Accept',
  };

  /// Đóng server và tunnel SSH
  Future<void> stopTunnel() async {
    _isRunning = false;
    try {
      if (_sshClient != null && !_sshClient!.isClosed) {
        _sshClient!.close();
        _sshClient = null;
      }
    } catch (e) {
      LoggerService().log('Lỗi đóng SSH client: $e', tag: 'TUNNEL');
    }

    try {
      if (_server != null) {
        await _server!.close(force: true);
        _server = null;
      }
    } catch (e) {
      LoggerService().log('Lỗi đóng HTTP server: $e', tag: 'TUNNEL');
    }
  }

  bool get isRunning => _isRunning;
}
