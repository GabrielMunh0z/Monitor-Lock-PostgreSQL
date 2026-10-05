import sys
import time
import socket
import threading
import os
import psycopg2

from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, render_template, jsonify


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# CAMINHOS DO PROJETO
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

CONFIG_DIR = BASE_DIR / "config"
OUTPUT_DIR = BASE_DIR / "output"

INSTANCES_FILE = CONFIG_DIR / "instances.txt"
PASSWORD_FILE = CONFIG_DIR / "pswd"
LOG_FILE = OUTPUT_DIR / "monitor-execution.log"


# ============================================================
# CONFIGURAÇÃO POSTGRESQL
# ============================================================

DB_PORT = int(os.environ.get("PG_PORT", "xxxx"))
DB_NAME = os.environ.get("PG_DATABASE", "SEU_BANCO")

DB_USER = os.environ.get("PG_USER", "SEU_USUARIO")

CONNECT_TIMEOUT = 3


# ============================================================
# CONFIGURAÇÃO DO MONITOR
# ============================================================

MAX_CONEXOES = 20

# 20 minutos
INTERVALO_ATUALIZACAO = 1200


# ============================================================
# CACHE DO MONITOR
# ============================================================

monitor_lock = threading.Lock()

monitor_executando = False

ultima_atualizacao = None
proxima_atualizacao = None

resultados_cache = []

resumo_cache = {
    "total": 0,
    "normal": 0,
    "lock": 0,
    "timeout": 0,
    "erro": 0
}


# ============================================================
# QUERY DE LOCKS
# ============================================================

#Essa QUERY é generica funciona em qualquer banco POSTGRESQL
QUERY_LOCKS = """
SELECT
    blocked.pid AS blocked_pid,
    blocked.usename AS blocked_user,
    blocking.pid AS blocking_pid,
    blocking.usename AS blocking_user,
    now() - blocked.query_start AS waiting_time,
    blocked.query AS blocked_query,
    blocking.query AS blocking_query
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking
    ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
WHERE cardinality(pg_blocking_pids(blocked.pid)) > 0
ORDER BY waiting_time DESC
"""


# ============================================================
# LOG
# ============================================================

def log(mensagem):

    horario = datetime.now().strftime(
        "%d/%m/%Y %H:%M:%S"
    )

    print(
        f"{horario} {mensagem}"
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    try:

        with open(
            LOG_FILE,
            "a",
            encoding="utf-8"
        ) as arquivo:

            arquivo.write(
                f"{horario} {mensagem}\n"
            )

    except Exception as erro:

        print(
            f"[AVISO] Erro ao gravar log: {erro}"
        )


# ============================================================
# CARREGAR SENHA
# ============================================================

def carregar_senha():

    if not PASSWORD_FILE.exists():

        raise FileNotFoundError(
            f"Arquivo não encontrado: {PASSWORD_FILE}"
        )

    senha = PASSWORD_FILE.read_text(
        encoding="utf-8"
    ).strip()

    if not senha:

        raise ValueError(
            "O arquivo config/pswd está vazio."
        )

    return senha


# ============================================================
# CARREGAR FILIAIS
# ============================================================

def carregar_filiais():

    if not INSTANCES_FILE.exists():

        raise FileNotFoundError(
            f"Arquivo não encontrado: {INSTANCES_FILE}"
        )

    filiais = []

    with open(
        INSTANCES_FILE,
        "r",
        encoding="utf-8"
    ) as arquivo:

        for linha in arquivo:

            linha = linha.strip()

            if not linha:
                continue

            try:

                filiais.append(
                    int(linha)
                )

            except ValueError:

                log(
                    f"[AVISO] Filial inválida: {linha}"
                )

    return sorted(
        set(filiais)
    )


# ============================================================
# HOST
# ============================================================

def gerar_host(filial):

    return os.environ.get("PG_HOST_PATTERN", "db{filial:03d}.exemplo.local").format(filial=filial)


# ============================================================
# IDENTIFICAR TIMEOUT
# ============================================================

def erro_timeout(erro):

    texto = str(erro).lower()

    termos = [
        "timeout",
        "timed out",
        "tempo limite",
        "connection timed out"
    ]

    return any(
        termo in texto
        for termo in termos
    )


# ============================================================
# CONVERTER TEMPO
# ============================================================

def formatar_timedelta(valor):

    if valor is None:
        return None

    try:

        total_segundos = int(
            valor.total_seconds()
        )

        horas = total_segundos // 3600

        minutos = (
            total_segundos % 3600
        ) // 60

        segundos = (
            total_segundos % 60
        )

        return (
            f"{horas:02d}:"
            f"{minutos:02d}:"
            f"{segundos:02d}"
        )

    except Exception:

        return str(valor)


# ============================================================
# VERIFICAR FILIAL
# ============================================================

def verificar_filial(filial, senha):

    host = gerar_host(filial)

    conexao = None

    inicio = time.monotonic()

    try:

        log(
            f"[TESTE] {filial:03d} | "
            f"{host} | conectando..."
        )

        # ====================================================
        # CONEXÃO POSTGRESQL
        # ====================================================

        conexao = psycopg2.connect(
            host=host,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=senha,
            connect_timeout=CONNECT_TIMEOUT
        )

        conexao.autocommit = True

        # ====================================================
        # QUERY
        # ====================================================

        with conexao.cursor() as cursor:

            cursor.execute(
                QUERY_LOCKS
            )

            locks_db = cursor.fetchall()

        duracao = (
            time.monotonic()
            - inicio
        )

        # ====================================================
        # LOCK
        # ====================================================

        if locks_db:

            locks = []

            for lock in locks_db:

                (
                    blocked_pid,
                    blocked_user,
                    blocking_pid,
                    blocking_user,
                    waiting_time,
                    blocked_query,
                    blocking_query
                ) = lock

                locks.append({
                    "blocked_pid": blocked_pid,
                    "blocked_user": blocked_user,
                    "blocking_pid": blocking_pid,
                    "blocking_user": blocking_user,
                    "waiting_time": formatar_timedelta(
                        waiting_time
                    ),
                    "blocked_query": blocked_query,
                    "blocking_query": blocking_query
                })

            maior_espera = formatar_timedelta(
                locks_db[0][4]
            )

            log(
                f"[LOCK] {filial:03d} | "
                f"{host} | "
                f"{len(locks)} lock(s)"
            )

            return {
                "filial": f"{filial:03d}",
                "host": host,
                "status": "LOCK",
                "quantidade_locks": len(locks),
                "maior_espera": maior_espera,
                "duracao": round(duracao, 2),
                "locks": locks,
                "erro": None
            }

        # ====================================================
        # NORMAL
        # ====================================================

        log(
            f"[OK] {filial:03d} | "
            f"{host} | SEM LOCK"
        )

        return {
            "filial": f"{filial:03d}",
            "host": host,
            "status": "NORMAL",
            "quantidade_locks": 0,
            "maior_espera": None,
            "duracao": round(duracao, 2),
            "locks": [],
            "erro": None
        }

    # ========================================================
    # SOCKET TIMEOUT
    # ========================================================

    except socket.timeout as erro:

        duracao = (
            time.monotonic()
            - inicio
        )

        log(
            f"[TIMEOUT] {filial:03d} | "
            f"{host}"
        )

        return {
            "filial": f"{filial:03d}",
            "host": host,
            "status": "TIMEOUT",
            "quantidade_locks": 0,
            "maior_espera": None,
            "duracao": round(duracao, 2),
            "locks": [],
            "erro": str(erro)
        }

    # ========================================================
    # OUTROS ERROS
    # ========================================================

    except Exception as erro:

        duracao = (
            time.monotonic()
            - inicio
        )

        if erro_timeout(erro):

            status = "TIMEOUT"

            log(
                f"[TIMEOUT] {filial:03d} | "
                f"{host}"
            )

        else:

            status = "ERRO"

            log(
                f"[ERRO] {filial:03d} | "
                f"{host} | {erro}"
            )

        return {
            "filial": f"{filial:03d}",
            "host": host,
            "status": status,
            "quantidade_locks": 0,
            "maior_espera": None,
            "duracao": round(duracao, 2),
            "locks": [],
            "erro": str(erro)
        }

    # ========================================================
    # SEMPRE FECHAR CONEXÃO
    # ========================================================

    finally:

        if conexao is not None:

            try:

                conexao.close()

                log(
                    f"[CLOSE] {filial:03d} | "
                    f"{host}"
                )

            except Exception as erro:

                log(
                    f"[AVISO] Erro fechando "
                    f"{host}: {erro}"
                )


# ============================================================
# MONITORAR TODAS AS FILIAIS
# ============================================================

def monitorar_filiais():

    senha = carregar_senha()

    filiais = carregar_filiais()

    resultados = []

    log(
        "=" * 70
    )

    log(
        "INICIANDO COLETA POSTGRESQL"
    )

    log(
        f"Filiais: {len(filiais)} | "
        f"Workers: {MAX_CONEXOES}"
    )

    inicio = time.monotonic()

    # ========================================================
    # ATÉ 20 FILIAIS SIMULTÂNEAS
    # ========================================================

    with ThreadPoolExecutor(
        max_workers=MAX_CONEXOES
    ) as executor:

        tarefas = {
            executor.submit(
                verificar_filial,
                filial,
                senha
            ): filial

            for filial in filiais
        }

        for tarefa in as_completed(
            tarefas
        ):

            filial = tarefas[tarefa]

            try:

                resultado = tarefa.result()

            except Exception as erro:

                host = gerar_host(
                    filial
                )

                resultado = {
                    "filial": f"{filial:03d}",
                    "host": host,
                    "status": "ERRO",
                    "quantidade_locks": 0,
                    "maior_espera": None,
                    "duracao": 0,
                    "locks": [],
                    "erro": str(erro)
                }

            resultados.append(
                resultado
            )

    resultados.sort(
        key=lambda item:
        int(item["filial"])
    )

    duracao = (
        time.monotonic()
        - inicio
    )

    log(
        f"COLETA FINALIZADA EM "
        f"{duracao:.2f}s"
    )

    return resultados


# ============================================================
# GERAR RESUMO
# ============================================================

def gerar_resumo(resultados):

    return {

        "total": len(resultados),

        "normal": sum(
            1
            for item in resultados
            if item["status"] == "NORMAL"
        ),

        "lock": sum(
            1
            for item in resultados
            if item["status"] == "LOCK"
        ),

        "timeout": sum(
            1
            for item in resultados
            if item["status"] == "TIMEOUT"
        ),

        "erro": sum(
            1
            for item in resultados
            if item["status"] == "ERRO"
        )
    }


# ============================================================
# EXECUTAR COLETA
# ============================================================

def executar_coleta():

    global monitor_executando
    global resultados_cache
    global resumo_cache
    global ultima_atualizacao
    global proxima_atualizacao

    # Impede duas coletas simultâneas
    with monitor_lock:

        if monitor_executando:

            log(
                "[AVISO] Coleta já está em execução."
            )

            return

        monitor_executando = True

    try:

        resultados = monitorar_filiais()

        resumo = gerar_resumo(
            resultados
        )

        agora = datetime.now()

        proxima = (
            agora
            + timedelta(
                seconds=INTERVALO_ATUALIZACAO
            )
        )

        # Atualiza o cache somente quando
        # a coleta completa terminar.
        with monitor_lock:

            resultados_cache = resultados
            resumo_cache = resumo
            ultima_atualizacao = agora
            proxima_atualizacao = proxima

        log(
            f"[RESUMO] "
            f"Total={resumo['total']} | "
            f"Normal={resumo['normal']} | "
            f"Lock={resumo['lock']} | "
            f"Timeout={resumo['timeout']} | "
            f"Erro={resumo['erro']}"
        )

    except Exception as erro:

        log(
            f"[ERRO GERAL] {erro}"
        )

    finally:

        with monitor_lock:

            monitor_executando = False


# ============================================================
# LOOP AUTOMÁTICO - 20 MINUTOS
# ============================================================

def loop_monitor():

    while True:

        inicio_ciclo = time.monotonic()

        executar_coleta()

        duracao = (
            time.monotonic()
            - inicio_ciclo
        )

        # Mantém aproximadamente 20 minutos
        # entre o início de cada rodada.
        espera = max(
            1,
            INTERVALO_ATUALIZACAO - duracao
        )

        log(
            f"Próxima coleta em "
            f"{espera:.0f} segundos."
        )

        time.sleep(
            espera
        )


# ============================================================
# ROTAS FLASK
# ============================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# ============================================================
# API - STATUS
# ============================================================

@app.route("/api/status")
def api_status():

    with monitor_lock:

        dados = {
            "monitor_executando": monitor_executando,

            "ultima_atualizacao":
                ultima_atualizacao.strftime(
                    "%d/%m/%Y %H:%M:%S"
                )
                if ultima_atualizacao
                else None,

            "proxima_atualizacao":
                proxima_atualizacao.strftime(
                    "%d/%m/%Y %H:%M:%S"
                )
                if proxima_atualizacao
                else None,

            "intervalo_segundos":
                INTERVALO_ATUALIZACAO,

            "resumo":
                resumo_cache.copy(),

            "filiais":
                list(resultados_cache)
        }

    return jsonify(
        dados
    )


# ============================================================
# API - DETALHE DA FILIAL
# ============================================================

@app.route("/api/filial/<filial>")
def api_filial(filial):

    filial = filial.zfill(3)

    with monitor_lock:

        for resultado in resultados_cache:

            if resultado["filial"] == filial:

                return jsonify(
                    resultado
                )

    return jsonify({
        "erro": "Filial não encontrada."
    }), 404


# ============================================================
# INICIAR THREAD DO MONITOR
# ============================================================

def iniciar_thread_monitor():

    thread = threading.Thread(
        target=loop_monitor,
        daemon=True,
        name="postgres-monitor"
    )

    thread.start()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    print()
    print("=" * 70)
    print("POSTGRESQL LOCK MONITOR - N3")
    print("=" * 70)
    print(f"Workers     : {MAX_CONEXOES}")
    print(
        f"Atualização : "
        f"{INTERVALO_ATUALIZACAO // 60} minutos"
    )
    print("Painel      : http://127.0.0.1:5000")
    print("=" * 70)
    print()

    # Inicia uma única thread de coleta
    iniciar_thread_monitor()

    # IMPORTANTE:
    # use_reloader=False evita o Flask iniciar
    # duas vezes a thread do monitor.
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=True,
        use_reloader=False
    )
# Ao usar Gunicorn com 1 worker, defina MONITOR_AUTOSTART=1.
if __name__ != "__main__" and os.environ.get("MONITOR_AUTOSTART") == "1":
    iniciar_thread_monitor()
