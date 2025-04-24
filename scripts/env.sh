BASEDIR="$(dirname $BASH_SOURCE)"
BASEDIR="$(readlink -f "$BASEDIR")"

export GOOGLE_APPLICATION_CREDENTIALS="$(readlink -f $BASEDIR/../.service-account.json)"
export PROJECT_ID="$(gcloud config get-value project)"

unset BASEDIR
