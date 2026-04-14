"""
Doris MySQL Protocol Client — 纯 Python 标准库实现，无需 pip 安装任何依赖
支持 Doris / MySQL 协议的基本连接、查询、结果解析
"""
import socket
import struct
import hashlib
import json
import re


class DorisClient:
    def __init__(self, host: str, port: int, user: str, password: str = "", database: str = ""):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.sock: socket.socket | None = None
        self._seq = 0

    # ------------------------------------------------------------------ #
    #  Low-level packet I/O
    # ------------------------------------------------------------------ #

    def _recv_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Connection closed by server")
            buf += chunk
        return buf

    def _read_packet(self) -> bytes:
        header = self._recv_exact(4)
        length = struct.unpack("<I", header[:3] + b"\x00")[0]
        self._seq = header[3]
        return self._recv_exact(length)

    def _send_packet(self, data: bytes, seq: int | None = None):
        if seq is None:
            self._seq += 1
        else:
            self._seq = seq
        length_bytes = struct.pack("<I", len(data))[:3]
        self.sock.sendall(length_bytes + bytes([self._seq]) + data)

    # ------------------------------------------------------------------ #
    #  Connection / Handshake
    # ------------------------------------------------------------------ #

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=10)
        self._handshake()

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def _handshake(self):
        pkt = self._read_packet()
        pos = 0
        # Protocol version (1 byte)
        proto_ver = pkt[pos]; pos += 1
        # Server version (null-terminated)
        nul = pkt.index(b"\x00", pos); pos = nul + 1
        # Connection id (4 bytes)
        pos += 4
        # Auth plugin data part 1 (8 bytes)
        salt1 = pkt[pos:pos + 8]; pos += 8
        # Filler (1 byte)
        pos += 1
        # Capability flags lower (2 bytes)
        caps_lo = struct.unpack("<H", pkt[pos:pos + 2])[0]; pos += 2
        # Character set (1 byte)
        pos += 1
        # Status flags (2 bytes)
        pos += 2
        # Capability flags upper (2 bytes)
        caps_hi = struct.unpack("<H", pkt[pos:pos + 2])[0]; pos += 2
        caps = caps_lo | (caps_hi << 16)
        # Length of auth-plugin-data
        if caps & 0x00080000:  # CLIENT_PLUGIN_AUTH
            auth_data_len = pkt[pos]; pos += 1
        else:
            pos += 1
        # Reserved (10 bytes)
        pos += 10
        # Auth plugin data part 2
        part2_len = max(13, auth_data_len - 8) if caps & 0x00080000 else 13
        salt2 = pkt[pos:pos + part2_len - 1]; pos += part2_len
        salt = salt1 + salt2

        # Build HandshakeResponse41
        client_caps = (
            0x00000001 |  # CLIENT_LONG_PASSWORD
            0x00000200 |  # CLIENT_PROTOCOL_41
            0x00008000 |  # CLIENT_SECURE_CONNECTION
            0x00080000 |  # CLIENT_PLUGIN_AUTH
            0x00000004 |  # CLIENT_LONG_FLAG
            0x00000008 |  # CLIENT_CONNECT_WITH_DB
            0x00020000 |  # CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            0x00200000    # CLIENT_CONNECT_ATTRS
        )
        auth_resp = self._mysql_native_password(self.password, salt)
        response = struct.pack("<IIB23s", client_caps, 16777216, 33, b"\x00" * 23)
        response += (self.user.encode("utf-8") + b"\x00")
        response += bytes([len(auth_resp)]) + auth_resp
        if self.database:
            response += (self.database.encode("utf-8") + b"\x00")
        response += b"mysql_native_password\x00"
        # Connect attributes (empty)
        response += b"\x00"

        self._send_packet(response, seq=1)
        result = self._read_packet()
        if result[0] == 0xFF:
            errno = struct.unpack("<H", result[1:3])[0]
            msg = result[9:].decode("utf-8", errors="replace")
            raise ConnectionError(f"Auth failed [{errno}]: {msg}")
        # OK or switch request — treat both as success for no-password root

    @staticmethod
    def _mysql_native_password(password: str, salt: bytes) -> bytes:
        if not password:
            return b""
        sha1 = hashlib.sha1
        p1 = sha1(password.encode("utf-8")).digest()
        p2 = sha1(p1).digest()
        p3 = sha1(salt + p2).digest()
        return bytes(a ^ b for a, b in zip(p1, p3))

    # ------------------------------------------------------------------ #
    #  Query execution
    # ------------------------------------------------------------------ #

    def execute(self, sql: str) -> list[dict]:
        """Execute SQL and return list of dicts."""
        self._seq = -1
        self._send_packet(b"\x03" + sql.encode("utf-8"), seq=0)
        pkt = self._read_packet()

        if pkt[0] == 0x00:
            return []
        if pkt[0] == 0xFF:
            errno = struct.unpack("<H", pkt[1:3])[0]
            msg = pkt[9:].decode("utf-8", errors="replace")
            raise Exception(f"Query error [{errno}]: {msg}")

        # Result set: first byte is column count (lenenc int)
        col_count = self._read_lenenc_int(pkt, 0)[0]
        columns = []
        for _ in range(col_count):
            col_pkt = self._read_packet()
            col_name = self._parse_column_name(col_pkt)
            columns.append(col_name)

        # EOF after column defs
        eof = self._read_packet()

        rows = []
        while True:
            row_pkt = self._read_packet()
            if row_pkt[0] == 0xFE and len(row_pkt) < 9:  # EOF
                break
            if row_pkt[0] == 0xFF:
                errno = struct.unpack("<H", row_pkt[1:3])[0]
                msg = row_pkt[9:].decode("utf-8", errors="replace")
                raise Exception(f"Row error [{errno}]: {msg}")
            row = {}
            pos = 0
            for col in columns:
                if row_pkt[pos] == 0xFB:
                    row[col] = None; pos += 1
                else:
                    val_len, consumed = self._read_lenenc_int(row_pkt, pos)
                    pos += consumed
                    val = row_pkt[pos:pos + val_len].decode("utf-8", errors="replace")
                    row[col] = val; pos += val_len
            rows.append(row)
        return rows

    def execute_many(self, sql: str):
        """Execute DDL / DML, ignore result."""
        try:
            self.execute(sql)
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  Protocol helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _read_lenenc_int(data: bytes, pos: int) -> tuple[int, int]:
        b = data[pos]
        if b < 0xFB:
            return b, 1
        if b == 0xFC:
            return struct.unpack("<H", data[pos + 1:pos + 3])[0], 3
        if b == 0xFD:
            return struct.unpack("<I", data[pos + 1:pos + 4] + b"\x00")[0], 4
        return struct.unpack("<Q", data[pos + 1:pos + 9])[0], 9

    @staticmethod
    def _parse_column_name(pkt: bytes) -> str:
        """Parse column name from Column Definition packet."""
        pos = 0
        # catalog, db, table, org_table — each lenenc string
        for _ in range(4):
            if pos >= len(pkt):
                break
            length = pkt[pos]; pos += 1 + length
        # name (actual column name)
        if pos < len(pkt):
            length = pkt[pos]; pos += 1
            return pkt[pos:pos + length].decode("utf-8", errors="replace")
        return "col"

    # ------------------------------------------------------------------ #
    #  Context manager
    # ------------------------------------------------------------------ #

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()


# ------------------------------------------------------------------ #
#  Simple connection factory
# ------------------------------------------------------------------ #

def get_connection(host="10.26.20.3", port=19030, user="root", password="", database="") -> DorisClient:
    c = DorisClient(host, port, user, password, database)
    c.connect()
    return c
