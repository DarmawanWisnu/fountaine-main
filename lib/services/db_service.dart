import 'dart:convert';
import 'package:crypto/crypto.dart';
import 'package:path/path.dart';
import 'package:sqflite/sqflite.dart';
import 'package:uuid/uuid.dart';

import '../domain/telemetry.dart';

class DatabaseService {
  DatabaseService._();
  static final DatabaseService instance = DatabaseService._();

  static const _dbName = 'data.db';
  static const _dbVersion = 1;
  static Database? _db;

  Future<void> init() async {
    final dbPath = await getDatabasesPath();
    final path = join(dbPath, _dbName);

    _db = await openDatabase(
      path,
      version: _dbVersion,
      onCreate: (db, version) async {
        await db.execute('''
          CREATE TABLE IF NOT EXISTS telemetry (
            row_id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL,
            ingest_time INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            id INTEGER,
            ppm REAL,
            ph REAL,
            tempC REAL,
            humidity REAL,
            waterTemp REAL,
            waterLevel REAL,
            pH_reducer INTEGER,
            add_water INTEGER,
            nutrients_adder INTEGER,
            humidifier INTEGER,
            ex_fan INTEGER,
            isDefault INTEGER,
            payload_hash TEXT UNIQUE
          );
        ''');

        await db.execute('''
          CREATE INDEX IF NOT EXISTS idx_telemetry_device_time
          ON telemetry(device_id, ingest_time);
        ''');
      },
    );

    print("[DB] Initialized at: $path");
  }

  Database get _database {
    if (_db == null) {
      throw Exception('Database not initialized! Call init() first.');
    }
    return _db!;
  }

  Future<void> insertTelemetry(
    String deviceId,
    Telemetry data, {
    String? rawJson,
  }) async {
    final db = _database;
    final uuid = const Uuid().v4();
    final now = DateTime.now().millisecondsSinceEpoch;
    final jsonStr = rawJson ?? jsonEncode(data.toJson());
    final hash = sha1.convert(utf8.encode(jsonStr)).toString();

    final map = {
      'row_id': uuid,
      'device_id': deviceId,
      'ingest_time': now,
      'payload_json': jsonStr,
      'id': data.id,
      'ppm': data.ppm,
      'ph': data.ph,
      'tempC': data.tempC,
      'humidity': data.humidity,
      'waterTemp': data.waterTemp,
      'waterLevel': data.waterLevel,
      'pH_reducer': data.pHReducer ? 1 : 0,
      'add_water': data.addWater ? 1 : 0,
      'nutrients_adder': data.nutrientsAdder ? 1 : 0,
      'humidifier': data.humidifier ? 1 : 0,
      'ex_fan': data.exFan ? 1 : 0,
      'isDefault': data.isDefault ? 1 : 0,
      'payload_hash': hash,
    };

    try {
      final inserted = await db.insert(
        'telemetry',
        map,
        conflictAlgorithm: ConflictAlgorithm.ignore,
      );

      print(
        "[DB] INSERT ${inserted == 0 ? "(DUPLICATE)" : "OK"} for KIT=$deviceId | ph=${data.ph} | ppm=${data.ppm}",
      );

      final count = Sqflite.firstIntValue(
        await db.rawQuery("SELECT COUNT(*) FROM telemetry WHERE device_id=?", [
          deviceId,
        ]),
      );

      print("[DB] Current rows for $deviceId → $count");
    } catch (e) {
      print("[DB] INSERT ERROR for $deviceId → $e");
    }
  }

  Future<List<Telemetry>> getAllTelemetry(String deviceId) async {
    final db = _database;
    final rows = await db.query(
      'telemetry',
      where: 'device_id = ?',
      whereArgs: [deviceId],
      orderBy: 'ingest_time DESC',
    );

    return rows
        .map(
          (r) => Telemetry.fromJson(
            jsonDecode(r['payload_json'] as String) as Map<String, dynamic>,
          ),
        )
        .toList();
  }

  Future<List<Map<String, dynamic>>> getEntriesWithTs(String deviceId) async {
    final db = _database;
    final rows = await db.query(
      'telemetry',
      columns: ['ingest_time', 'payload_json'],
      where: 'device_id = ?',
      whereArgs: [deviceId],
      orderBy: 'ingest_time DESC',
    );

    return rows.map((r) {
      final ts = (r['ingest_time'] as int?) ?? 0;
      final payload = r['payload_json'] as String;
      final t = Telemetry.fromJson(jsonDecode(payload) as Map<String, dynamic>);
      return {'t': t, 'ts': ts};
    }).toList();
  }

  Future<void> pruneOlderThan(Duration age) async {
    final db = _database;
    final threshold = DateTime.now().subtract(age).millisecondsSinceEpoch;
    await db.delete(
      'telemetry',
      where: 'ingest_time < ?',
      whereArgs: [threshold],
    );

    print("[DB] Pruned rows older than $age");
  }

  Future<void> close() async {
    await _db?.close();
    print("[DB] Closed");
    _db = null;
  }
}
