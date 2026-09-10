import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:mobile_scanner/mobile_scanner.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:http/http.dart' as http;
import '../../core/database/database_helper.dart';
import '../../l10n/app_localizations.dart';
import 'package:audire_reader/src/rust/api/sync.dart' as rust_sync;
import '../../services/logger_service.dart';

class MobileSyncScreen extends StatefulWidget {
  const MobileSyncScreen({super.key});

  @override
  State<MobileSyncScreen> createState() => _MobileSyncScreenState();
}

class _MobileSyncScreenState extends State<MobileSyncScreen> {
  String? _scannedUrl;
  bool _isLoading = false;
  MobileScannerController? _scannerController;

  @override
  void initState() {
    super.initState();
    _initScanner();
  }

  @override
  void dispose() {
    _scannerController?.dispose();
    super.dispose();
  }

  void _initScanner() {
    _scannerController = MobileScannerController(
      detectionSpeed: DetectionSpeed.normal,
      facing: CameraFacing.back,
      torchEnabled: false,
    );
  }

  void _onDetect(BarcodeCapture capture) {
    if (_scannedUrl != null || _isLoading) return;
    final List<Barcode> barcodes = capture.barcodes;
    for (final barcode in barcodes) {
      final rawValue = barcode.rawValue;
      if (rawValue != null &&
          rawValue.startsWith('http') &&
          rawValue.contains('/config')) {
        HapticFeedback.lightImpact();
        setState(() {
          _scannedUrl = rawValue;
        });
        _sendConfig();
        break;
      }
    }
  }

  Future<void> _showManualUrlDialog() async {
    final textController = TextEditingController();
    final clipboardData = await Clipboard.getData(Clipboard.kTextPlain);
    if (clipboardData?.text != null &&
        clipboardData!.text!.startsWith('http') &&
        clipboardData.text!.contains('/config')) {
      textController.text = clipboardData.text!;
    }

    if (!mounted) return;

    final url = await showDialog<String>(
      context: context,
      builder: (context) {
        final isDark = Theme.of(context).brightness == Brightness.dark;
        return AlertDialog(
          backgroundColor: isDark ? const Color(0xFF1E1E1E) : Colors.white,
          shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(20)),
          title: const Text('Nhập liên kết máy nhận', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 16)),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              const Text(
                'Nhập hoặc dán địa chỉ endpoint từ màn hình "Nhận cấu hình" của máy nhận (ví dụ: http://192.168.1.15:16021/config)',
                style: TextStyle(fontSize: 12),
              ),
              const SizedBox(height: 14),
              TextField(
                controller: textController,
                autofocus: true,
                decoration: InputDecoration(
                  hintText: 'http://192.168.x.x:port/config',
                  hintStyle: const TextStyle(fontSize: 12),
                  filled: true,
                  contentPadding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
                  border: OutlineInputBorder(borderRadius: BorderRadius.circular(12)),
                  suffixIcon: IconButton(
                    icon: const Icon(Icons.paste_rounded, size: 20),
                    onPressed: () async {
                      final data = await Clipboard.getData(Clipboard.kTextPlain);
                      if (data?.text != null) {
                        textController.text = data!.text!.trim();
                      }
                    },
                  ),
                ),
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.pop(context),
              child: const Text('Hủy'),
            ),
            ElevatedButton(
              onPressed: () {
                final input = textController.text.trim();
                if (input.isNotEmpty && input.startsWith('http')) {
                  Navigator.pop(context, input);
                }
              },
              child: const Text('Gửi cấu hình'),
            ),
          ],
        );
      },
    );

    if (url != null && url.isNotEmpty) {
      setState(() {
        _scannedUrl = url;
      });
      _sendConfig();
    }
  }

  Future<void> _sendConfig() async {
    if (_scannedUrl == null) return;
    setState(() {
      _isLoading = true;
    });

    try {
      final db = await DatabaseHelper.getInstance();
      final settings = await db.getSettings();
      String webDavPassword = '';
      try {
        webDavPassword = (await rust_sync.getWebdavPassword()) ?? '';
      } catch (e) {
        debugPrint('[MobileSyncScreen] Error loading WebDAV password: $e');
      }

      // Đóng gói cấu hình hiện tại
      final payload = {
        'webDavUrl': settings.webDavUrl,
        'webDavUsername': settings.webDavUsername,
        'webDavPassword': webDavPassword,
        'deviceName': settings.deviceName ?? 'Mobile Device',
        'settings': {
          'fontSize': settings.fontSize,
          'speechRate': settings.speechRate,
          'selectedVoiceName': settings.selectedVoiceName,
          'selectedVoiceLocale': settings.selectedVoiceLocale,
          'ttsProvider': settings.ttsProvider,
          'openAiTtsEndpoint': settings.openAiTtsEndpoint,
          'openAiTtsApiKey': settings.openAiTtsApiKey,
          'openAiTtsModel': settings.openAiTtsModel,
          'fontFamily': settings.fontFamily,
          'fontWeight': settings.fontWeight,
          'themeMode': settings.themeMode,
          'appLocale': settings.appLocale,
          'lineHeight': settings.lineHeight,
          'paragraphSpacing': settings.paragraphSpacing,
          'textAlignment': settings.textAlignment,
          'sideMargin': settings.sideMargin,
          'customBackgroundColor': settings.customBackgroundColor,
          'customTextColor': settings.customTextColor,
          'primaryColorHex': settings.primaryColorHex,
          'openLastReadOnLaunch': settings.openLastReadOnLaunch,
          'hotkeyNextParagraph': settings.hotkeyNextParagraph,
          'hotkeyPrevParagraph': settings.hotkeyPrevParagraph,
          'hotkeyNextChapter': settings.hotkeyNextChapter,
          'hotkeyPrevChapter': settings.hotkeyPrevChapter,
          'hotkeyPlayPauseTts': settings.hotkeyPlayPauseTts,
          'hotkeyOpenChapter': settings.hotkeyOpenChapter,
          'hotkeyOpenSetting': settings.hotkeyOpenSetting,
          'hotkeyBossKey': settings.hotkeyBossKey,
          'bossKeyAction': settings.bossKeyAction,
          'autoCheckUpdate': settings.autoCheckUpdate,
          'bgmEnabled': settings.bgmEnabled,
          'bgmVolume': settings.bgmVolume,
          'bgmLoopMode': settings.bgmLoopMode,
          'bgmProviderId': settings.bgmProviderId,
          'sortBy': settings.sortBy,
          'developerMode': settings.developerMode,
          'enableDebugLogs': settings.enableDebugLogs,
          'enableWebDavDebug': settings.enableWebDavDebug,
        },
      };

      LoggerService().log('Đang gửi config tới $_scannedUrl...', tag: 'SYNC');
      final response = await http
          .post(
            Uri.parse(_scannedUrl!),
            headers: {'Content-Type': 'application/json'},
            body: json.encode(payload),
          )
          .timeout(const Duration(seconds: 15));

      if (mounted) {
        if (response.statusCode == 200) {
          final resData = json.decode(response.body);
          if (resData['success'] == true || resData['status'] == 'ok') {
            ScaffoldMessenger.of(context).showSnackBar(
              SnackBar(
                content: Text(
                  AppLocalizations.of(context)?.shareConfigSuccess ??
                      'Chia sẻ cấu hình thành công!',
                ),
                backgroundColor: Colors.green,
              ),
            );
            Navigator.pop(context, true);
          } else {
            throw Exception(
              resData['message'] ?? 'Phản hồi từ máy nhận không hợp lệ.',
            );
          }
        } else {
          throw Exception('Máy nhận trả về mã lỗi: ${response.statusCode}');
        }
      }
    } catch (e) {
      LoggerService().log('Lỗi gửi cấu hình: $e', tag: 'SYNC', level: LogLevel.error);
      if (mounted) {
        showDialog(
          context: context,
          builder: (context) => AlertDialog(
            title: Text(
              AppLocalizations.of(context)?.sendConfigErrorTitle ??
                  'Lỗi gửi cấu hình',
            ),
            content: Text(
              AppLocalizations.of(context)?.sendConfigErrorDesc(e.toString()) ??
                  'Không thể kết nối và truyền cấu hình tới thiết bị nhận. Chi tiết: $e',
            ),
            actions: [
              TextButton(
                onPressed: () {
                  Navigator.pop(context);
                  setState(() {
                    _scannedUrl = null;
                    _isLoading = false;
                  });
                },
                child: Text(AppLocalizations.of(context)?.rescan ?? 'Quét lại'),
              ),
            ],
          ),
        );
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);

    return Scaffold(
      backgroundColor: Colors.black,
      appBar: AppBar(
        title: Text(
          AppLocalizations.of(context)?.shareConfigQrScanner ??
              'Chia sẻ cấu hình (Quét QR)',
          style: const TextStyle(
            fontWeight: FontWeight.bold,
            color: Colors.white,
          ),
        ),
        elevation: 0,
        backgroundColor: Colors.transparent,
        foregroundColor: Colors.white,
        actions: [
          IconButton(
            icon: const Icon(Icons.link_rounded),
            tooltip: 'Nhập / Dán liên kết thủ công',
            onPressed: _isLoading ? null : _showManualUrlDialog,
          ),
        ],
      ),
      body: Stack(
        children: [
          // Camera Scanner
          MobileScanner(
            controller: _scannerController,
            onDetect: _onDetect,
            errorBuilder: (context, error, child) {
              if (error.errorCode == MobileScannerErrorCode.permissionDenied) {
                return Center(
                  child: Padding(
                    padding: const EdgeInsets.all(24.0),
                    child: Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        const Icon(
                          Icons.camera_alt_outlined,
                          color: Colors.white54,
                          size: 64,
                        ),
                        const SizedBox(height: 16),
                        Text(
                          AppLocalizations.of(context)?.needCameraPermission ??
                              'Cần quyền truy cập Camera để quét QR code',
                          style: const TextStyle(
                            color: Colors.white,
                            fontSize: 16,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                        const SizedBox(height: 12),
                        Text(
                          AppLocalizations.of(context)?.cameraPermissionDesc ??
                              'Vui lòng cấp quyền camera cho ứng dụng trong phần Cài đặt thiết bị để tiếp tục.',
                          textAlign: TextAlign.center,
                          style: const TextStyle(
                            color: Colors.white70,
                            fontSize: 13,
                          ),
                        ),
                        const SizedBox(height: 24),
                        ElevatedButton(
                          onPressed: () async {
                            await openAppSettings();
                          },
                          style: ElevatedButton.styleFrom(
                            backgroundColor: theme.colorScheme.primary,
                            foregroundColor: Colors.white,
                            padding: const EdgeInsets.symmetric(
                              horizontal: 24,
                              vertical: 12,
                            ),
                          ),
                          child: Text(
                            AppLocalizations.of(context)?.openSettings ??
                                'Mở Cài đặt',
                          ),
                        ),
                        const SizedBox(height: 12),
                        OutlinedButton.icon(
                          onPressed: _showManualUrlDialog,
                          icon: const Icon(Icons.link_rounded, color: Colors.white),
                          label: const Text(
                            'Nhập / Dán liên kết thủ công',
                            style: TextStyle(color: Colors.white),
                          ),
                        ),
                      ],
                    ),
                  ),
                );
              }
              return Center(
                child: Text(
                  error.errorDetails?.message ?? 'Lỗi khởi động camera',
                  style: const TextStyle(color: Colors.white),
                ),
              );
            },
          ),

          // Chỉ vẽ các overlay quét nếu không bị lỗi quyền
          ValueListenableBuilder<MobileScannerState>(
            valueListenable: _scannerController!,
            builder: (context, state, child) {
              if (state.error != null &&
                  state.error!.errorCode ==
                      MobileScannerErrorCode.permissionDenied) {
                return const SizedBox.shrink();
              }
              return Stack(
                children: [
                  // Overlay Khung quét
                  Center(
                    child: Container(
                      width: 250,
                      height: 250,
                      decoration: BoxDecoration(
                        border: Border.all(
                          color: theme.colorScheme.primary,
                          width: 3,
                        ),
                        borderRadius: BorderRadius.circular(24),
                      ),
                    ),
                  ),

                  // Overlay Text hướng dẫn
                  Positioned(
                    bottom: 80,
                    left: 20,
                    right: 20,
                    child: Center(
                      child: Container(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 16,
                          vertical: 10,
                        ),
                        decoration: BoxDecoration(
                          color: Colors.black54,
                          borderRadius: BorderRadius.circular(12),
                        ),
                        child: Text(
                          AppLocalizations.of(context)?.scanQrCodeInstruction ??
                              'Di chuyển camera để quét mã QR kết nối của máy nhận',
                          textAlign: TextAlign.center,
                          style: const TextStyle(
                            color: Colors.white,
                            fontSize: 13,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                      ),
                    ),
                  ),
                ],
              );
            },
          ),

          // Loading Indicator khi đang gửi
          if (_isLoading)
            Container(
              color: Colors.black87,
              child: Center(
                child: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    const CircularProgressIndicator(color: Colors.white),
                    const SizedBox(height: 16),
                    Text(
                      AppLocalizations.of(context)?.sendingConfig ??
                          'Đang truyền cấu hình tới thiết bị nhận...',
                      style: const TextStyle(
                        color: Colors.white,
                        fontSize: 14,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ],
                ),
              ),
            ),
        ],
      ),
    );
  }
}
