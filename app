"""
Dashboard de Preços FIPE — App Flask
Lê os preços do Postgres da Aiven (tabela fipe_precos). Quando o cache está
velho, o próprio app busca dados novos na BrasilAPI (refresh_stale_series).
Não depende de Kafka.

Variáveis de ambiente esperadas:
  AIVEN_DB_URL   -> string de conexão fornecida pela Aiven
  PORT           -> porta HTTP (default 8080)
  CACHE_HOURS    -> horas antes de buscar dados novos na API (default 24)
"""
import datetime
import os
import time

import requests
from flask import Flask, jsonify, render_template

import fipe_client as fipe

app = Flask(__name__)

CACHE_HOURS = int(os.environ.get("CACHE_HOURS", 24))
START_TIME = time.time()

# Em produção o app roda via gunicorn (`gunicorn app:app`), que nunca executa o
# bloco `if __name__ == "__main__":`; por isso as tabelas são criadas aqui.
fipe.init_db()


def log_visit(path: str):
    with fipe.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO fipe_visits (path) VALUES (%s);", (path,))
        conn.commit()


def is_cache_stale(codigo: str) -> bool:
    with fipe.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(fetched_at) AS last_fetch FROM fipe_precos WHERE codigo_fipe = %s;",
                (codigo,),
            )
            row = cur.fetchone()
    if not row or not row["last_fetch"]:
        return True
    age = datetime.datetime.now(datetime.timezone.utc) - row["last_fetch"]
    return age > datetime.timedelta(hours=CACHE_HOURS)


def refresh_series(codigo: str):
    points = fipe.fetch_series_from_fipe(codigo)
    fipe.upsert_series(codigo, points)


def refresh_stale_series():
    for codigo in fipe.SERIES:
        if is_cache_stale(codigo):
            try:
                refresh_series(codigo)
            except (requests.RequestException, ValueError) as exc:  # noqa: BLE001
                app.logger.warning("Falha ao atualizar %s: %s", codigo, exc)


def get_series_data(codigo: str):
    """Últimos pontos em ordem cronológica."""
    with fipe.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ref_date, valor AS value, marca, modelo, ano_modelo
                FROM (
                    SELECT * FROM fipe_precos
                    WHERE codigo_fipe = %s
                    ORDER BY ref_date DESC
                    LIMIT %s
                ) t
                ORDER BY ref_date ASC;
                """,
                (codigo, fipe.POINTS_PER_SERIES),
            )
            return cur.fetchall()


def get_dashboard_data():
    refresh_stale_series()

    indicators = []
    for codigo, meta in fipe.SERIES.items():
        rows = get_series_data(codigo)
        latest = rows[-1] if rows else None
        previous = rows[-2] if len(rows) > 1 else None
        delta = None
        if latest and previous:
            delta = round(float(latest["value"]) - float(previous["value"]), meta["casas"])

        nome = meta["name"] or (
            f'{latest["marca"]} {latest["modelo"]} {latest["ano_modelo"]}' if latest else codigo
        )
        indicators.append(
            {
                "code": codigo,
                "name": nome,
                "unit": meta["unit"],
                "latest_value": round(float(latest["value"]), meta["casas"]) if latest else None,
                "latest_date": latest["ref_date"].strftime("%m/%Y") if latest else None,
                "delta": delta,
                "labels": [r["ref_date"].strftime("%m/%Y") for r in rows],
                "values": [round(float(r["value"]), meta["casas"]) for r in rows],
            }
        )

    with fipe.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM fipe_visits;")
            total_visits = cur.fetchone()["total"]

    return {
        "indicators": indicators,
        "total_visits": total_visits,
        "uptime_seconds": int(time.time() - START_TIME),
        "server_time": datetime.datetime.utcnow().isoformat() + "Z",
        "cache_hours": CACHE_HOURS,
    }


@app.route("/")
def dashboard():
    log_visit("/")
    data = get_dashboard_data()
    return render_template("dashboard.html", data=data)


@app.route("/api/indicators")
def api_indicators():
    log_visit("/api/indicators")
    return jsonify(get_dashboard_data())


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Força a atualização de todos os veículos, ignorando a idade do cache."""
    for codigo in fipe.SERIES:
        try:
            refresh_series(codigo)
        except (requests.RequestException, ValueError) as exc:  # noqa: BLE001
            return jsonify({"status": "error", "series": codigo, "detail": str(exc)}), 502
    return jsonify({"status": "ok", "refreshed": list(fipe.SERIES.keys())})


@app.route("/healthz")
def healthz():
    """Usado pelo pipeline de CI/CD e pela plataforma de deploy (health check)."""
    try:
        with fipe.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        return jsonify({"status": "ok"}), 200
    except Exception as exc:  # noqa: BLE001
        return jsonify({"status": "error", "detail": str(exc)}), 503


if __name__ == "__main__":
    refresh_stale_series()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
"""
Consumer — assina o tópico Kafka e grava cada evento na tabela bcb_series
do Postgres (a mesma que o dashboard lê). Dois modos de uso:

  Contínuo (para rodar como Background Worker no Render):
      python consumer.py

  Em lote, com timeout (para rodar como job agendado no GitHub Actions —
  ver .github/workflows/kafka-consume-batch.yml):
      python consumer.py --once --timeout 30

Como os dois processos usam o MESMO group_id (KAFKA_GROUP_ID, default
"bcb-consumer"), o Kafka nunca entrega a mesma mensagem para ambos ao
mesmo tempo — o job em lote funciona como rede de segurança para quando
o worker contínuo estiver fora do ar, sem duplicar processamento.
"""

import argparse
import json
import sys
import time

import bcb_client as bcb
import kafka_common


def handle_message(msg) -> None:
    event = json.loads(msg.value)
    series_code = int(event["series_code"])
    if series_code not in bcb.SERIES:
        print(f"[consumer] série desconhecida ignorada: {series_code}", file=sys.stderr)
        return

    import datetime

    point = {
        "ref_date": datetime.date.fromisoformat(event["ref_date"]),
        "value": float(event["value"]),
    }
    bcb.upsert_series(series_code, [point])
    print(f"[consumer] gravado: série={series_code} data={point['ref_date']} valor={point['value']}")


def run_once(timeout_seconds: int) -> None:
    consumer = kafka_common.get_consumer()
    deadline = time.time() + timeout_seconds
    count = 0
    try:
        while time.time() < deadline:
            batch = consumer.poll(timeout_ms=2000, max_records=50)
            if not batch:
                continue
            for records in batch.values():
                for msg in records:
                    handle_message(msg)
                    count += 1
    finally:
        consumer.close()
    print(f"[consumer] modo lote concluído — {count} evento(s) processado(s)")


def run_forever() -> None:
    consumer = kafka_common.get_consumer()
    print("[consumer] aguardando eventos continuamente... (Ctrl+C para sair)")
    try:
        for msg in consumer:
            handle_message(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="processa por um período fixo e sai")
    parser.add_argument("--timeout", type=int, default=30, help="segundos no modo --once")
    args = parser.parse_args()

    bcb.init_db()

    if args.once:
        run_once(args.timeout)
    else:
        run_forever()


if __name__ == "__main__":
    main()

FROM python:3.12-slim

WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

ENV PORT=8080
EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--timeout", "60", "app:app"]

"""Cliente de preços FIPE + acesso ao PostgreSQL.

Fontes de dados (tenta nesta ordem; se uma falhar, usa a próxima):
  1. API v2 da FIPE (fipe.parallelum.com.br / fipe.api.br) — sem cadastro, 500 consultas/dia
  2. BrasilAPI (brasilapi.com.br/api/fipe) — sem chave

Variáveis de ambiente:
  AIVEN_DB_URL  -> string de conexão do PostgreSQL (obrigatória)
  FIPE_TOKEN    -> (opcional) token gratuito de fipe.api.br; dobra o limite diário
"""
import datetime
import os
import re
import time
from contextlib import contextmanager

import psycopg2
import requests
from psycopg2.extras import RealDictCursor, execute_values

# Fonte 1: API v2 da FIPE (o endereço mudou de dono; tenta os dois).
FIPE_BASES = [
    "https://fipe.parallelum.com.br/api/v2",
    "https://fipe.api.br/api/v2",
]
# Fonte 2: BrasilAPI
BRASILAPI_BASE = "https://brasilapi.com.br/api/fipe"

TIMEOUT = 20  # segundos por requisição

# Veículos acompanhados no dashboard. A chave é o código FIPE.
#   ano_modelo: ano que você quer acompanhar (None = o mais novo disponível)
#   name:       nome mostrado no dashboard (None = usa o modelo vindo da API)
SERIES = {
    "095007-6": {"name": "Dolphin EV GS (Elétrico)", "ano_modelo": None, "unit": "R$", "casas": 2},
    "008030-6": {"name": "100 2.8 V6", "ano_modelo": None, "unit": "R$", "casas": 2},
    "001494-0": {"name": "ARGO DRIVE 1.0 6V Flex", "ano_modelo": None, "unit": "R$", "casas": 2},
}

N_TABELAS = 6            # quantos meses de histórico buscar
POINTS_PER_SERIES = 12   # pontos máximos por gráfico

MESES = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8, "setembro": 9,
    "outubro": 10, "novembro": 11, "dezembro": 12,
}


# ----------------------------------------------------------------- banco ---
@contextmanager
def get_connection():
    conn = psycopg2.connect(os.environ["AIVEN_DB_URL"], cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS fipe_precos (
                    codigo_fipe  TEXT        NOT NULL,
                    ano_modelo   INTEGER     NOT NULL,
                    ref_date     DATE        NOT NULL,
                    valor        NUMERIC(12,2) NOT NULL,
                    marca        TEXT,
                    modelo       TEXT,
                    combustivel  TEXT,
                    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (codigo_fipe, ano_modelo, ref_date)
                );
                CREATE TABLE IF NOT EXISTS fipe_visits (
                    id         SERIAL PRIMARY KEY,
                    path       TEXT NOT NULL,
                    visited_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
        conn.commit()


def upsert_series(codigo, points):
    """Grava os pontos de um código FIPE (insere ou atualiza)."""
    unicos = {}
    for p in points:  # evita 'ON CONFLICT' duplicado dentro do mesmo comando
        unicos[(p["ano_modelo"], p["ref_date"])] = p
    if not unicos:
        return
    linhas = [
        (codigo, p["ano_modelo"], p["ref_date"], p["valor"],
         p["marca"], p["modelo"], p["combustivel"])
        for p in unicos.values()
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO fipe_precos
                    (codigo_fipe, ano_modelo, ref_date, valor, marca, modelo, combustivel)
                VALUES %s
                ON CONFLICT (codigo_fipe, ano_modelo, ref_date) DO UPDATE
                SET valor = EXCLUDED.valor, marca = EXCLUDED.marca,
                    modelo = EXCLUDED.modelo, combustivel = EXCLUDED.combustivel,
                    fetched_at = now();
                """,
                linhas,
            )
        conn.commit()


# ------------------------------------------------------------ utilitários ---
def _get(base, caminho, params=None, headers=None, tentativas=3):
    """GET com novas tentativas em erro 5xx/rede. Erros 4xx não são repetidos."""
    ultimo_erro = None
    for n in range(tentativas):
        try:
            resp = requests.get(f"{base}{caminho}", params=params,
                                headers=headers, timeout=TIMEOUT)
            if resp.status_code >= 500:
                raise requests.HTTPError(f"{resp.status_code} em {resp.url}", response=resp)
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as erro:
            ultimo_erro = erro
            resp_erro = getattr(erro, "response", None)
            if resp_erro is not None and resp_erro.status_code < 500:
                raise
            time.sleep(2 * (n + 1))
    raise ultimo_erro


def parse_valor(texto):
    """'R$ 59.900,00' -> 59900.0"""
    limpo = re.sub(r"[^\d,]", "", str(texto or ""))
    if not limpo:
        raise ValueError(f"valor inválido: {texto!r}")
    return float(limpo.replace(",", "."))


def parse_mes(texto):
    """'novembro de 2026', 'novembro/2026' ou '2026-11' -> date(2026, 11, 1)"""
    t = str(texto or "").lower()
    iso = re.search(r"(\d{4})-(\d{2})", t)
    if iso:
        return datetime.date(int(iso.group(1)), int(iso.group(2)), 1)
    ano = re.search(r"(\d{4})", t)
    mes = next((n for nome, n in MESES.items() if nome in t), None)
    if not ano or not mes:
        raise ValueError(f"mês de referência não reconhecido: {texto!r}")
    return datetime.date(int(ano.group(1)), mes, 1)


# ------------------------------------------------ fonte 1: API v2 da FIPE ---
def _fget(caminho, params=None):
    """Consulta a API v2 tentando cada endereço base."""
    headers = {"X-Subscription-Token": os.environ["FIPE_TOKEN"]} if os.environ.get("FIPE_TOKEN") else None
    ultimo = None
    for base in FIPE_BASES:
        try:
            return _get(base, caminho, params, headers)
        except requests.RequestException as erro:
            ultimo = erro
    raise ultimo


def _ano_de(item):
    m = re.match(r"(\d{4,5})", str(item.get("code", "")))
    return int(m.group(1)) if m else 0


def _escolher_ano(anos, ano_modelo):
    if not anos:
        raise ValueError("a API não devolveu anos para esse código")
    if ano_modelo is not None:
        cand = [a for a in anos if _ano_de(a) == ano_modelo]
        if not cand:
            disp = ", ".join(str(_ano_de(a)) for a in anos)
            raise ValueError(f"ano {ano_modelo} não existe para esse código (disponíveis: {disp})")
        return cand[0]["code"]
    normais = [a for a in anos if _ano_de(a) < 32000] or anos  # 32000 = zero km
    return max(normais, key=_ano_de)["code"]


def _ponto_fipe(veiculo, preco, mes, ano_fallback):
    return {
        "ano_modelo": veiculo.get("modelYear") or ano_fallback,
        "ref_date": parse_mes(mes),
        "valor": parse_valor(preco),
        "marca": veiculo.get("brand"),
        "modelo": veiculo.get("model"),
        "combustivel": veiculo.get("fuel"),
    }


def _fonte_fipe(codigo, meta):
    anos = _fget(f"/cars/{codigo}/years")
    ano_id = _escolher_ano(anos, meta["ano_modelo"])
    base = f"/cars/{codigo}/years/{ano_id}"
    ano_num = _ano_de({"code": ano_id})

    # 1) histórico de preços em uma chamada só
    try:
        v = _fget(base + "/history")
        pontos = [
            _ponto_fipe(v, h.get("price"), h.get("month"), ano_num)
            for h in (v.get("priceHistory") or [])
        ]
        if pontos:
            return pontos[:N_TABELAS]
    except (requests.RequestException, ValueError):
        pass  # histórico pode exigir token; segue para o plano seguinte

    # 2) um preço por mês de referência
    pontos = []
    try:
        referencias = _fget("/references")[:N_TABELAS]
    except requests.RequestException:
        referencias = []
    for ref in referencias:
        try:
            v = _fget(base, {"reference": ref["code"]})
            pontos.append(_ponto_fipe(v, v.get("price"),
                                      v.get("referenceMonth") or ref.get("month"), ano_num))
        except (requests.RequestException, ValueError):
            continue
    if pontos:
        return pontos

    # 3) só o preço atual
    v = _fget(base)
    return [_ponto_fipe(v, v.get("price"), v.get("referenceMonth"), ano_num)]


# ------------------------------------------------------ fonte 2: BrasilAPI ---
def listar_tabelas(limite=N_TABELAS):
    """Tabelas de referência (meses) mais recentes da BrasilAPI."""
    return _get(BRASILAPI_BASE, "/tabelas/v1")[:limite]


def listar_marcas(tipo="carros"):
    return _get(BRASILAPI_BASE, f"/marcas/v1/{tipo}")


def listar_veiculos(tipo, codigo_marca):
    return _get(BRASILAPI_BASE, f"/veiculos/v1/{tipo}/{codigo_marca}")


def _fonte_brasilapi(codigo, meta):
    pontos = []
    try:
        tabelas = listar_tabelas()
    except requests.RequestException:
        tabelas = [{"codigo": None, "mes": None}]  # sem lista de meses: só o preço atual
    for tabela in tabelas:
        params = {"tabela_referencia": tabela["codigo"]} if tabela["codigo"] else None
        try:
            itens = _get(BRASILAPI_BASE, f"/preco/v1/{codigo}", params)
        except requests.RequestException:
            continue
        if meta["ano_modelo"] is not None:
            itens = [i for i in itens if i.get("anoModelo") == meta["ano_modelo"]]
        if not itens:
            continue
        item = max(itens, key=lambda i: i.get("anoModelo") or 0)
        try:
            ref = parse_mes(item.get("mesReferencia") or tabela.get("mes"))
        except ValueError:
            if not tabela.get("mes"):
                raise
            ref = parse_mes(tabela.get("mes"))
        pontos.append({
            "ano_modelo": item.get("anoModelo"),
            "ref_date": ref,
            "valor": parse_valor(item.get("valor")),
            "marca": item.get("marca"),
            "modelo": item.get("modelo"),
            "combustivel": item.get("combustivel"),
        })
    return pontos


# ------------------------------------------------------------ entrada única ---
def fetch_series_from_fipe(codigo):
    """Preço do veículo nos últimos meses. Tenta cada fonte até uma funcionar."""
    meta = SERIES[codigo]
    erros = []
    for nome, fonte in (("API FIPE", _fonte_fipe), ("BrasilAPI", _fonte_brasilapi)):
        try:
            pontos = fonte(codigo, meta)
            if pontos:
                return pontos
            erros.append(f"{nome}: sem dados")
        except (requests.RequestException, ValueError, KeyError) as erro:
            erros.append(f"{nome}: {erro}")
    raise ValueError(f"nenhum preço para {codigo} -> " + " | ".join(erros))


if __name__ == "__main__":
    # python fipe_client.py   -> testa as fontes com os códigos de SERIES
    for cod in SERIES:
        try:
            for p in fetch_series_from_fipe(cod)[:3]:
                print(cod, p)
        except ValueError as e:
            print(cod, "ERRO:", e)
"""
Helper de conexão com o Aiven for Apache Kafka via SASL_SSL.

Variáveis de ambiente esperadas:
  KAFKA_BOOTSTRAP_SERVERS -> host:port (saída "kafka_bootstrap_servers" do Terraform)
  KAFKA_USERNAME           -> usuário SASL (saída "kafka_username")
  KAFKA_PASSWORD           -> senha SASL (saída "kafka_password")
  KAFKA_CA_CERT_PEM        -> conteúdo do certificado CA (saída "kafka_ca_cert")
  KAFKA_TOPIC               -> nome do tópico (default: bcb-indicadores)
"""

import os
import tempfile
from kafka import KafkaProducer, KafkaConsumer

TOPIC = os.environ.get("KAFKA_TOPIC", "bcb-indicadores")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "bcb-consumer")


def _ca_cert_path() -> str:
    """Grava o certificado CA (vindo de uma variável de ambiente/secret) em
    um arquivo temporário, pois as libs de Kafka esperam um caminho de arquivo."""
    pem = os.environ.get("KAFKA_CA_CERT_PEM")
    if not pem:
        raise RuntimeError("KAFKA_CA_CERT_PEM não configurada")
    fd, path = tempfile.mkstemp(suffix=".pem")
    with os.fdopen(fd, "w") as f:
        f.write(pem)
    return path


def _common_config() -> dict:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    username = os.environ.get("KAFKA_USERNAME")
    password = os.environ.get("KAFKA_PASSWORD")
    if not all([bootstrap, username, password]):
        raise RuntimeError(
            "KAFKA_BOOTSTRAP_SERVERS, KAFKA_USERNAME e KAFKA_PASSWORD são obrigatórias"
        )
    return {
        "bootstrap_servers": bootstrap,
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": username,
        "sasl_plain_password": password,
        "ssl_cafile": _ca_cert_path(),
    }


def get_producer() -> KafkaProducer:
    cfg = _common_config()
    return KafkaProducer(
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        **cfg,
    )


def get_consumer() -> KafkaConsumer:
    cfg = _common_config()
    return KafkaConsumer(
        TOPIC,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
        **cfg,
    )

"""
Producer — busca o ponto mais recente de cada série do BCB e publica no
tópico Kafka. Roda como job agendado (ver .github/workflows/kafka-produce.yml)
ou manualmente: `python producer.py`.

Não escreve no Postgres — essa é a responsabilidade do consumer.py, que
assina o mesmo tópico. O producer só materializa o evento "novo dado
disponível" no Kafka.
"""

import json
import sys

import bcb_client as bcb
import kafka_common


def main():
    producer = kafka_common.get_producer()
    published = 0

    for series_code in bcb.SERIES:
        try:
            # Só o ponto mais recente — o histórico completo já foi
            # semeado pelo cache batch do app.py na primeira execução.
            points = bcb.fetch_series_from_bcb(series_code, n=1)
        except Exception as exc:  # noqa: BLE001
            print(f"[producer] falha ao buscar série {series_code}: {exc}", file=sys.stderr)
            continue

        for p in points:
            event = {
                "series_code": series_code,
                "ref_date": p["ref_date"].isoformat(),
                "value": p["value"],
            }
            producer.send(
                kafka_common.TOPIC,
                key=str(series_code),
                value=json.dumps(event),
            )
            published += 1
            print(f"[producer] publicado: {event}")

    producer.flush(timeout=10)
    producer.close()
    print(f"[producer] concluído — {published} evento(s) publicado(s)")


if __name__ == "__main__":
    main()

Flask==3.0.3
psycopg2-binary==2.9.9
gunicorn==22.0.0
requests==2.32.3
kafka-python==2.0.2

* { box-sizing: border-box; }
body {
  font-family: -apple-system, "Segoe UI", Roboto, sans-serif;
  background: #0f1220;
  color: #e6e6f0;
  margin: 0;
  padding: 2rem;
}
header h1 { margin-bottom: 0.2rem; }
.subtitle { color: #9a9ac0; margin-top: 0; }

.cards {
  display: flex;
  gap: 1rem;
  margin: 1.5rem 0;
  flex-wrap: wrap;
}
.card {
  background: #1a1e33;
  border: 1px solid #2c2f52;
  border-radius: 12px;
  padding: 1rem 1.5rem;
  min-width: 180px;
  display: flex;
  flex-direction: column;
}
.card .label { color: #9a9ac0; font-size: 0.85rem; }
.card .value { font-size: 1.8rem; font-weight: 700; color: #a5b4fc; }
.card .value.small { font-size: 1rem; }
.card .unit { font-size: 1rem; font-weight: 500; color: #9a9ac0; margin-left: 0.3rem; }
.card .meta { font-size: 0.8rem; color: #6b6b91; margin-top: 0.3rem; }
.card.muted .value { color: #cbd5e1; }
.up { color: #4ade80; }
.down { color: #f87171; }

.chart-container, .table-container {
  background: #1a1e33;
  border: 1px solid #2c2f52;
  border-radius: 12px;
  padding: 1.5rem;
  margin-bottom: 1.5rem;
}

table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: 0.5rem; border-bottom: 1px solid #2c2f52; }
th { color: #9a9ac0; font-weight: 600; }

footer { color: #6b6b91; text-align: center; margin-top: 2rem; }
footer a { color: #a5b4fc; }

<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>Dashboard — Indicadores Econômicos (Dados Abertos BCB)</title>
  <link rel="stylesheet" href="{{ url_for('static', filename='style.css') }}">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
</head>
<body>
  <header>
    <h1>📈 Indicadores Econômicos do Brasil</h1>
    <p class="subtitle">
      Dados abertos do Tabela Fipe · cache renovado a cada {{ data.cache_hours }}h ·
      persistidos em PostgreSQL gerenciado pela Aiven.io
    </p>
  </header>

  <section class="cards">
    {% for ind in data.indicators %}
    <div class="card">
      <span class="label">{{ ind.name }}</span>
      <span class="value">
        {{ ind.latest_value if ind.latest_value is not none else "—" }}
        <span class="unit">{{ ind.unit }}</span>
      </span>
      <span class="meta">
        {% if ind.latest_date %}ref. {{ ind.latest_date }}{% else %}sem dados ainda{% endif %}
        {% if ind.delta is not none %}
          · <span class="{{ 'up' if ind.delta > 0 else ('down' if ind.delta < 0 else '') }}">
            {{ "%+.2f"|format(ind.delta) }}
          </span>
        {% endif %}
      </span>
    </div>
    {% endfor %}
    <div class="card muted">
      <span class="label">Acessos ao dashboard</span>
      <span class="value">{{ data.total_visits }}</span>
      <span class="meta">uptime: {{ data.uptime_seconds }}s</span>
    </div>
  </section>

  <section class="chart-container">
    <h2>Séries históricas (últimos {{ data.indicators[0].labels|length }} pontos)</h2>
    <canvas id="indicatorsChart" height="90"></canvas>
  </section>

  <footer>
    <small>
      Atualiza automaticamente a cada 60s · Fonte:
      <a href="https://dadosabertos.bcb.gov.br/" target="_blank" rel="noopener">dadosabertos.bcb.gov.br</a>
      · <a href="/api/indicators">/api/indicators</a> · <a href="/healthz">/healthz</a>
    </small>
  </footer>

  <script>
    const indicators = {{ data.indicators | tojson }};
    const colors = ["#4f46e5", "#16a34a", "#dc2626"];

    const datasets = indicators.map((ind, i) => ({
      label: ind.name + " (" + ind.unit + ")",
      data: ind.values,
      borderColor: colors[i % colors.length],
      backgroundColor: colors[i % colors.length] + "26",
      tension: 0.3,
      yAxisID: i === 0 ? "y" : "y" + i,
      fill: false
    }));

    const ctx = document.getElementById('indicatorsChart');
    new Chart(ctx, {
      type: 'line',
      data: {
        labels: indicators[0] ? indicators[0].labels : [],
        datasets: datasets
      },
      options: {
        responsive: true,
        interaction: { mode: 'index', intersect: false },
        plugins: { legend: { display: true, position: 'bottom' } },
        scales: { y: { beginAtZero: false } }
      }
    });

    setTimeout(() => location.reload(), 60000);
  </script>
</body>
</html>
