#!/usr/bin/env bash
# =============================================================================
# ImmoScan - Script de backup PostgreSQL
#
# Effectue un pg_dump de la base de donnees immoscan avec :
# - Compression gzip
# - Horodatage dans le nom du fichier
# - Retention : 7 backups quotidiens + 4 hebdomadaires
# - Nettoyage automatique des anciens backups
# - Code de sortie non-zero en cas d'erreur
# =============================================================================

set -euo pipefail

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

BACKUP_DIR="${BACKUP_DIR:-/backups/immoscan}"
DATABASE_URL="${DATABASE_URL:-postgresql://immoscan:password@localhost:5432/immoscan}"
DAILY_RETENTION=7
WEEKLY_RETENTION=4

# Extraire les informations de connexion depuis DATABASE_URL
# Format: postgresql://user:password@host:port/dbname
parse_database_url() {
    local url="$1"
    # Supprimer le prefixe postgresql://
    local stripped="${url#postgresql://}"

    # Extraire user:password@host:port/dbname
    local userpass="${stripped%%@*}"
    local hostportdb="${stripped#*@}"

    DB_USER="${userpass%%:*}"
    DB_PASSWORD="${userpass#*:}"
    DB_HOST="${hostportdb%%:*}"

    local portdb="${hostportdb#*:}"
    DB_PORT="${portdb%%/*}"
    DB_NAME="${portdb#*/}"
}

parse_database_url "$DATABASE_URL"

# Horodatage
DATE_STAMP=$(date +"%Y%m%d_%H%M%S")
DAY_OF_WEEK=$(date +"%u")  # 1=lundi, 7=dimanche

# Noms de fichier
DAILY_FILENAME="immoscan_daily_${DATE_STAMP}.sql.gz"
WEEKLY_FILENAME="immoscan_weekly_${DATE_STAMP}.sql.gz"

# ------------------------------------------------------------------
# Fonctions
# ------------------------------------------------------------------

log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

# Creer le repertoire de backup si necessaire
ensure_backup_dir() {
    if [ ! -d "$BACKUP_DIR" ]; then
        log_info "Creation du repertoire de backup: $BACKUP_DIR"
        mkdir -p "$BACKUP_DIR"
    fi
}

# Effectuer le pg_dump
do_backup() {
    local output_file="$1"
    local full_path="${BACKUP_DIR}/${output_file}"

    log_info "Demarrage du backup: $output_file"

    export PGPASSWORD="$DB_PASSWORD"

    if pg_dump \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        --format=custom \
        --compress=9 \
        --no-owner \
        --no-privileges \
        | gzip > "$full_path"; then

        unset PGPASSWORD

        # Verifier que le fichier n'est pas vide
        if [ ! -s "$full_path" ]; then
            log_error "Fichier de backup vide: $full_path"
            rm -f "$full_path"
            return 1
        fi

        local size
        size=$(du -h "$full_path" | cut -f1)
        log_info "Backup termine: $output_file ($size)"
        return 0
    else
        unset PGPASSWORD
        log_error "Echec du pg_dump pour: $output_file"
        rm -f "$full_path"
        return 1
    fi
}

# Nettoyer les anciens backups quotidiens (garder les N plus recents)
cleanup_daily() {
    log_info "Nettoyage des backups quotidiens (retention: $DAILY_RETENTION)"

    local count
    count=$(find "$BACKUP_DIR" -name "immoscan_daily_*.sql.gz" -type f | wc -l)

    if [ "$count" -gt "$DAILY_RETENTION" ]; then
        local to_delete=$((count - DAILY_RETENTION))
        find "$BACKUP_DIR" -name "immoscan_daily_*.sql.gz" -type f \
            | sort \
            | head -n "$to_delete" \
            | while read -r file; do
                log_info "Suppression ancien backup quotidien: $(basename "$file")"
                rm -f "$file"
            done
    fi
}

# Nettoyer les anciens backups hebdomadaires (garder les N plus recents)
cleanup_weekly() {
    log_info "Nettoyage des backups hebdomadaires (retention: $WEEKLY_RETENTION)"

    local count
    count=$(find "$BACKUP_DIR" -name "immoscan_weekly_*.sql.gz" -type f | wc -l)

    if [ "$count" -gt "$WEEKLY_RETENTION" ]; then
        local to_delete=$((count - WEEKLY_RETENTION))
        find "$BACKUP_DIR" -name "immoscan_weekly_*.sql.gz" -type f \
            | sort \
            | head -n "$to_delete" \
            | while read -r file; do
                log_info "Suppression ancien backup hebdomadaire: $(basename "$file")"
                rm -f "$file"
            done
    fi
}

# ------------------------------------------------------------------
# Execution principale
# ------------------------------------------------------------------

main() {
    log_info "========================================="
    log_info "ImmoScan Backup - Debut"
    log_info "========================================="
    log_info "Base: $DB_NAME@$DB_HOST:$DB_PORT (user: $DB_USER)"
    log_info "Repertoire: $BACKUP_DIR"

    # Preparer le repertoire
    ensure_backup_dir

    # Backup quotidien
    if ! do_backup "$DAILY_FILENAME"; then
        log_error "Echec du backup quotidien"
        exit 1
    fi

    # Backup hebdomadaire (le dimanche)
    if [ "$DAY_OF_WEEK" = "7" ]; then
        log_info "Dimanche detecte: creation du backup hebdomadaire"
        if ! do_backup "$WEEKLY_FILENAME"; then
            log_error "Echec du backup hebdomadaire"
            exit 1
        fi
    fi

    # Nettoyage
    cleanup_daily
    cleanup_weekly

    # Resume
    log_info "========================================="
    log_info "ImmoScan Backup - Termine avec succes"
    log_info "Backups quotidiens: $(find "$BACKUP_DIR" -name "immoscan_daily_*.sql.gz" -type f | wc -l)/$DAILY_RETENTION"
    log_info "Backups hebdomadaires: $(find "$BACKUP_DIR" -name "immoscan_weekly_*.sql.gz" -type f | wc -l)/$WEEKLY_RETENTION"
    log_info "========================================="

    exit 0
}

main "$@"
