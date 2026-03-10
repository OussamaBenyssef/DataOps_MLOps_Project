#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════
# health_check.sh — Vérification rapide de tous les services
# DataOps/MLOps Pipeline
#
# Usage:
#   ./scripts/health_check.sh
#   make health
#
# Exit codes:
#   0 → Tous les services critiques sont UP
#   1 → Au moins un service critique est DOWN
# ══════════════════════════════════════════════════════════════════

set -euo pipefail

# ─── Couleurs ────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# ─── Compteurs ───────────────────────────────────────────────────
PASS=0
FAIL=0
WARN=0

# ─── Fonctions helpers ────────────────────────────────────────────
check_tcp() {
    local name="$1"
    local host="$2"
    local port="$3"
    local critical="${4:-true}"

    if nc -z -w 3 "$host" "$port" 2>/dev/null; then
        echo -e "  ${GREEN}✅${RESET} ${BOLD}${name}${RESET} (${host}:${port})"
        ((PASS++)) || true
    else
        if [ "$critical" = "true" ]; then
            echo -e "  ${RED}❌${RESET} ${BOLD}${name}${RESET} (${host}:${port}) ${RED}— CRITIQUE${RESET}"
            ((FAIL++)) || true
        else
            echo -e "  ${YELLOW}⚠️ ${RESET} ${BOLD}${name}${RESET} (${host}:${port}) — non critique"
            ((WARN++)) || true
        fi
    fi
}

check_http() {
    local name="$1"
    local url="$2"
    local critical="${3:-true}"
    local timeout="${4:-5}"

    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" \
        --connect-timeout "$timeout" \
        --max-time "$timeout" \
        "$url" 2>/dev/null || echo "000")

    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 400 ] 2>/dev/null; then
        echo -e "  ${GREEN}✅${RESET} ${BOLD}${name}${RESET} → ${url} [HTTP ${http_code}]"
        ((PASS++)) || true
    else
        if [ "$critical" = "true" ]; then
            echo -e "  ${RED}❌${RESET} ${BOLD}${name}${RESET} → ${url} ${RED}[HTTP ${http_code}] — CRITIQUE${RESET}"
            ((FAIL++)) || true
        else
            echo -e "  ${YELLOW}⚠️ ${RESET} ${BOLD}${name}${RESET} → ${url} [HTTP ${http_code}] — non critique"
            ((WARN++)) || true
        fi
    fi
}

# ══════════════════════════════════════════════════════════════════
# DÉBUT DU CHECK
# ══════════════════════════════════════════════════════════════════

echo ""
echo -e "${BOLD}${BLUE}╔══════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${BLUE}║       DataOps/MLOps — Health Check               ║${RESET}"
echo -e "${BOLD}${BLUE}╚══════════════════════════════════════════════════╝${RESET}"
echo -e "  $(date '+%Y-%m-%d %H:%M:%S UTC')"
echo ""

# ─── Transport (Kafka / Zookeeper) ───────────────────────────────
echo -e "${CYAN}📡 Transport${RESET}"
check_tcp  "Zookeeper"       localhost  2181   true
check_tcp  "Kafka (externe)" localhost  29092  true
echo ""

# ─── Stockage ─────────────────────────────────────────────────────
echo -e "${CYAN}🗄️  Stockage${RESET}"
check_tcp  "MongoDB"         localhost  27017  true
check_tcp  "PostgreSQL"      localhost  5432   true
echo ""

# ─── Traitement ───────────────────────────────────────────────────
echo -e "${CYAN}⚡ Traitement${RESET}"
check_http "Spark Master UI"  "http://localhost:8080"          true
echo ""

# ─── Orchestration ───────────────────────────────────────────────
echo -e "${CYAN}✈️  Orchestration${RESET}"
check_http "Airflow Webserver" "http://localhost:8081/health"  true
echo ""

# ─── ML ──────────────────────────────────────────────────────────
echo -e "${CYAN}🤖 ML${RESET}"
check_http "MLflow"           "http://localhost:5001"          true
check_http "FastAPI"          "http://localhost:8000/health"   false
echo ""

# ─── Observabilité ───────────────────────────────────────────────
echo -e "${CYAN}📊 Observabilité${RESET}"
check_http "Metabase"         "http://localhost:3000/api/health"  false
echo ""

# ─── DataHub ─────────────────────────────────────────────────────
echo -e "${CYAN}📋 Catalogue — DataHub${RESET}"
check_http "DataHub GMS"      "http://localhost:8082/config"   false
check_http "DataHub Frontend" "http://localhost:9002"          false
echo ""

# ══════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════
TOTAL=$((PASS + FAIL + WARN))

echo -e "${BOLD}${BLUE}══════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Résumé :${RESET}"
echo -e "  ${GREEN}✅ UP         : ${PASS}${RESET}"
echo -e "  ${YELLOW}⚠️  Non-critique: ${WARN}${RESET}"
echo -e "  ${RED}❌ DOWN       : ${FAIL}${RESET}"
echo -e "  Total         : ${TOTAL}"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo -e "  ${RED}${BOLD}❌ pipeline DÉGRADÉ — ${FAIL} service(s) critique(s) DOWN${RESET}"
    echo -e "${BOLD}${BLUE}══════════════════════════════════════════════════${RESET}"
    echo ""
    exit 1
elif [ "$WARN" -gt 0 ]; then
    echo ""
    echo -e "  ${YELLOW}${BOLD}⚠️  Pipeline opérationnel avec ${WARN} service(s) non-critique(s) down${RESET}"
    echo -e "${BOLD}${BLUE}══════════════════════════════════════════════════${RESET}"
    echo ""
    exit 0
else
    echo ""
    echo -e "  ${GREEN}${BOLD}✅ Tous les services sont opérationnels !${RESET}"
    echo -e "${BOLD}${BLUE}══════════════════════════════════════════════════${RESET}"
    echo ""
    exit 0
fi
