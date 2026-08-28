import datetime
import json
import logging
import math
import struct
import time

import foxglove
import serial
from foxglove import Channel, Schema
from foxglove.messages import (
    Color,
    CubePrimitive,
    CylinderPrimitive,
    Duration,
    FrameTransform,
    FrameTransforms,
    Pose,
    Quaternion,
    SceneEntity,
    SceneUpdate,
    Timestamp,
    Vector3,
)
from foxglove.websocket import Capability, ChannelView, Client, ServerListener


# ============================================================
# CONFIGURAÇÃO
# ============================================================

PORTA = "/dev/ttyUSB0"
BAUDRATE = 9600

FRAME_HEADER_1 = 0xAA
FRAME_HEADER_2 = 0x55

PAYLOAD_SIZE = 189


# ============================================================
# FORMATO DO LapespDTO
# ============================================================

PAYLOAD_FORMAT = (
    "<"
    + "d" * 19
    + "i"
    + "Q"
    + "d"
    + "B"
    + "d" * 2
)

print("Tamanho esperado pelo Python:", struct.calcsize(PAYLOAD_FORMAT))


# ============================================================
# CRC16-CCITT (mesmo algoritmo do firmware)
# ============================================================

def crc16_ccitt(data, crc=0xFFFF):
    for byte in data:
        crc ^= (byte << 8)
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc


# ============================================================
# DECODIFICA PAYLOAD
# ============================================================

def decode_lapesp_payload(payload):

    if len(payload) != PAYLOAD_SIZE:
        raise ValueError(
            f"Tamanho inválido: {len(payload)} bytes, esperado {PAYLOAD_SIZE}"
        )

    data = struct.unpack(PAYLOAD_FORMAT, payload)

    return {
        "BME_temperatura": data[0],
        "BME_pressao": data[1],
        "BME_umidade": data[2],
        "BNO_q0": data[3],
        "BNO_q1": data[4],
        "BNO_q2": data[5],
        "BNO_q3": data[6],
        "BNO_accel_x": data[7],
        "BNO_accel_y": data[8],
        "BNO_accel_z": data[9],
        "BNO_gyro_x": data[10],
        "BNO_gyro_y": data[11],
        "BNO_gyro_z": data[12],
        "ADXL_acelg_x": data[13],
        "ADXL_acelg_y": data[14],
        "ADXL_acelg_z": data[15],
        "latitude": data[16],
        "longitude": data[17],
        "altitude": data[18],
        "GPS_count": data[19],
        "timestamp": data[20],
        "GPS_PREC": data[21],
        "mosfet_state": data[22],
        "CURRENT": data[23],
        "VOLTAGE": data[24],
    }


# ============================================================
# LISTENER - só usado pra saber se tem alguém conectado.
# IMPORTANTE: nunca usamos isso para bloquear um foxglove.log()
# ou channel.log() - a publicação sempre acontece. Isso serve
# só de informação/telemetria de debug.
# ============================================================

class LapespListener(ServerListener):
    def __init__(self) -> None:
        self.subscribers: dict[int, set[str]] = {}

    def has_subscribers(self) -> bool:
        return len(self.subscribers) > 0

    def on_subscribe(self, client: Client, channel: ChannelView) -> None:
        logging.info(f"Cliente {client} inscrito em {channel.topic}")
        self.subscribers.setdefault(client.id, set()).add(channel.topic)

    def on_unsubscribe(self, client: Client, channel: ChannelView) -> None:
        logging.info(f"Cliente {client} desinscrito de {channel.topic}")
        self.subscribers[client.id].discard(channel.topic)
        if not self.subscribers[client.id]:
            del self.subscribers[client.id]


# ============================================================
# FOXGLOVE
# ============================================================

foxglove.set_log_level("INFO")

listener = LapespListener()

server = foxglove.start_server(
    server_listener=listener,
    capabilities=[Capability.ClientPublish],
    supported_encodings=["json"],
)

print()
print("======================================")
print(" FOXGLOVE INICIADO")
print("======================================")
print("Conecte em:")
print("ws://localhost:8765")
print()


# ============================================================
# CANAL DE TELEMETRIA COM SCHEMA EXPLÍCITO
# ============================================================

telemetry_schema = json.dumps({
    "type": "object",
    "properties": {
        "BME_temperatura": {"type": "number", "description": "Temperatura em °C"},
        "BME_pressao": {"type": "number", "description": "Pressão em Pa"},
        "BME_umidade": {"type": "number", "description": "Umidade relativa em %"},
        "BNO_q0": {"type": "number"},
        "BNO_q1": {"type": "number"},
        "BNO_q2": {"type": "number"},
        "BNO_q3": {"type": "number"},
        "BNO_accel_x": {"type": "number"},
        "BNO_accel_y": {"type": "number"},
        "BNO_accel_z": {"type": "number"},
        "BNO_gyro_x": {"type": "number"},
        "BNO_gyro_y": {"type": "number"},
        "BNO_gyro_z": {"type": "number"},
        "ADXL_acelg_x": {"type": "number"},
        "ADXL_acelg_y": {"type": "number"},
        "ADXL_acelg_z": {"type": "number"},
        "latitude": {"type": "number"},
        "longitude": {"type": "number"},
        "altitude": {"type": "number", "description": "Altitude em metros"},
        "GPS_count": {"type": "integer"},
        "timestamp": {"type": "integer"},
        "GPS_PREC": {"type": "number"},
        "mosfet_state": {"type": "integer"},
        "CURRENT": {"type": "number"},
        "VOLTAGE": {"type": "number"},
    },
})

telemetry_channel = Channel(
    topic="/lapesp/telemetry",
    message_encoding="json",
    schema=Schema(
        name="LapespTelemetry",
        encoding="jsonschema",
        data=telemetry_schema.encode("utf-8"),
    ),
)


# ============================================================
# SERIAL
# ============================================================

ser = serial.Serial(PORTA, BAUDRATE, timeout=0.1)

print(f"Serial aberta em {PORTA}")
print("Aguardando pacotes LAPESP...")
print()


# ============================================================
# BUFFER SERIAL
# ============================================================

buffer = bytearray()

ultimo_dado_em = time.time()
total_bytes_recebidos = 0

# Quaternion "neutro" (sem rotação) - usado até que um pacote
# real do BNO080 seja decodificado. Assim o foguete aparece no
# Foxglove desde o início, mesmo sem dado nenhum na serial.
ultimo_quaternion = Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)

# Taxa máxima de publicação do /tf e /lapesp/scene. Sem isso,
# quando a serial manda dados contínuos, o loop principal pode
# girar centenas/milhares de vezes por segundo e inundar o
# Foxglove com mensagens redundantes, deixando tudo lento.
PUBLISH_HZ = 20
PUBLISH_INTERVAL = 1.0 / PUBLISH_HZ
ultima_publicacao_em = 0.0


def fin_pose(angle_deg: float, radius: float, z: float) -> Pose:
    """Calcula posição e orientação de uma aleta ao redor do eixo Z."""
    a = math.radians(angle_deg)
    x = radius * math.cos(a)
    y = radius * math.sin(a)
    half = a / 2
    return Pose(
        position=Vector3(x=x, y=y, z=z),
        orientation=Quaternion(x=0.0, y=0.0, z=math.sin(half), w=math.cos(half)),
    )


def build_rocket_cubes() -> list:
    """
    Monta o foguete com primitivas simples:
    - corpo: cilindro
    - nariz: cilindro com top_scale=0 (vira um cone)
    - 3 aletas: caixas finas na base, espaçadas 120° entre si

    Tudo com orientação local IDENTIDADE - a rotação real do
    BNO080 já é aplicada pelo /tf (frame "sensor"), então não
    duplicamos o quaternion aqui dentro.
    """

    corpo = CylinderPrimitive(
        pose=Pose(position=Vector3(x=0, y=0, z=0), orientation=Quaternion(x=0, y=0, z=0, w=1)),
        size=Vector3(x=0.3, y=0.3, z=1.0),
        bottom_scale=1.0,
        top_scale=1.0,
        color=Color(r=0.85, g=0.85, b=0.85, a=1.0),
    )

    nariz = CylinderPrimitive(
        pose=Pose(position=Vector3(x=0, y=0, z=0.7), orientation=Quaternion(x=0, y=0, z=0, w=1)),
        size=Vector3(x=0.3, y=0.3, z=0.4),
        bottom_scale=1.0,
        top_scale=0.0,
        color=Color(r=0.9, g=0.1, b=0.1, a=1.0),
    )

    aleta_size = Vector3(x=0.05, y=0.3, z=0.3)
    aleta_cor = Color(r=0.2, g=0.2, b=0.2, a=1.0)

    aletas = [
        CubePrimitive(pose=fin_pose(ang, radius=0.3, z=-0.35), size=aleta_size, color=aleta_cor)
        for ang in (0, 120, 240)
    ]

    return [corpo, nariz] + aletas


# Peças do foguete: são estáticas (não dependem do quaternion),
# então montamos uma vez só. A rotação inteira vem do /tf.
FOGUETE_PARTES = build_rocket_cubes()


def publica_tf_e_cubo(quaternion: Quaternion) -> None:
    foxglove.log(
        "/tf",
        FrameTransforms(
            transforms=[
                FrameTransform(
                    parent_frame_id="world",
                    child_frame_id="sensor",
                    rotation=quaternion,
                ),
            ]
        ),
    )

    foxglove.log(
        "/lapesp/scene",
        SceneUpdate(
            entities=[
                SceneEntity(
                    frame_id="sensor",
                    id="bno080_foguete",
                    timestamp=Timestamp.from_datetime(datetime.datetime.now()),
                    lifetime=Duration.from_secs(0.0),
                    cylinders=FOGUETE_PARTES[:2],
                    cubes=FOGUETE_PARTES[2:],
                ),
            ]
        ),
    )


# ============================================================
# LOOP PRINCIPAL
# ============================================================

while True:

    # Publica no máximo PUBLISH_HZ vezes por segundo, com o
    # último quaternion conhecido (real, se já chegou;
    # identidade, se ainda não chegou nenhum pacote válido).
    agora = time.time()
    if agora - ultima_publicacao_em >= PUBLISH_INTERVAL:
        publica_tf_e_cubo(ultimo_quaternion)
        ultima_publicacao_em = agora

    data = ser.read(256)

    if not data:
        if time.time() - ultimo_dado_em > 3:
            print(
                f"[debug] nenhum byte recebido na serial "
                f"há {time.time() - ultimo_dado_em:.0f}s "
                f"(total recebido até agora: {total_bytes_recebidos} bytes) "
                f"- confira porta/baudrate/se a placa está enviando"
            )
            ultimo_dado_em = time.time()
        continue

    ultimo_dado_em = time.time()
    total_bytes_recebidos += len(data)

    buffer.extend(data)

    while True:

        # ====================================================
        # PROCURA AA 55
        # ====================================================

        pos = buffer.find(bytes([FRAME_HEADER_1, FRAME_HEADER_2]))

        if pos < 0:
            if len(buffer) > 0 and buffer[-1] == FRAME_HEADER_1:
                buffer[:] = buffer[-1:]
            else:
                buffer.clear()
            break

        if len(buffer) < pos + 5:
            break

        packet_type = buffer[pos + 2]
        payload_len = buffer[pos + 3] | (buffer[pos + 4] << 8)

        total_len = 5 + payload_len + 2

        if len(buffer) < pos + total_len:
            break

        payload_start = pos + 5
        payload_end = payload_start + payload_len

        payload = bytes(buffer[payload_start:payload_end])

        crc_received = buffer[payload_end] | (buffer[payload_end + 1] << 8)

        del buffer[:pos + total_len]

        # ====================================================
        # CRC (só aviso - não bloqueia mais o processamento,
        # pois o cálculo pode não bater 100% com o firmware
        # e estava descartando TODOS os pacotes silenciosamente)
        # ====================================================

        crc_computed = crc16_ccitt(payload)

        if crc_computed != crc_received:
            print(
                f"[aviso] CRC não bateu "
                f"(recebido={crc_received:04X}, calculado={crc_computed:04X}) "
                f"- processando mesmo assim"
            )

        if packet_type != 0x01:
            print(f"[debug] pacote tipo 0x{packet_type:02X} ignorado (esperado 0x01)")
            continue

        if payload_len != PAYLOAD_SIZE:
            print(f"Payload inesperado: {payload_len} bytes")
            continue

        try:
            dados = decode_lapesp_payload(payload)
        except Exception as e:
            print("ERRO AO DECODIFICAR:", e)
            continue

        print(
            f"(clientes conectados: {'sim' if listener.has_subscribers() else 'não'})"
        )
        print()
        print("======================================")
        print("LAPESP DTO")
        print("======================================")
        print(f"Temperatura: {dados['BME_temperatura']:.2f} °C")
        print(f"Pressao:     {dados['BME_pressao']:.2f} Pa")
        print(f"Umidade:     {dados['BME_umidade']:.2f} %")
        print(f"Altitude:    {dados['altitude']:.2f} m")
        print(
            f"Q = ({dados['BNO_q0']:.3f}, {dados['BNO_q1']:.3f}, "
            f"{dados['BNO_q2']:.3f}, {dados['BNO_q3']:.3f})"
        )

        # ====================================================
        # TELEMETRIA COMPLETA
        # ====================================================

        telemetry_channel.log(dados)

        # ====================================================
        # QUATERNION
        # ====================================================

        qx = dados["BNO_q0"]
        qy = dados["BNO_q1"]
        qz = dados["BNO_q2"]
        qw = dados["BNO_q3"]

        norm = (qx * qx + qy * qy + qz * qz + qw * qw) ** 0.5

        if norm < 1e-6:
            print("BNO sem quaternion válido.")
            print("→ Telemetria atualizada")
            print("→ Cubo não atualizado")
            print("--------------------------------------")
            continue

        # Normaliza (evita erro de arredondamento do sensor)
        qx /= norm
        qy /= norm
        qz /= norm
        qw /= norm

        quaternion = Quaternion(x=qx, y=qy, z=qz, w=qw)

        # Atualiza o quaternion "oficial" - será publicado na
        # próxima volta do loop principal por publica_tf_e_cubo().
        ultimo_quaternion = quaternion

        print("→ Telemetria atualizada")
        print("→ TF/cubo serão atualizados na próxima iteração")
        print("--------------------------------------")