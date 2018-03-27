BRANCH="$1"
REPO_SLUG="$2"
SSH_URL="$3@$4"
GIT_URL="${5:-https://github.com}/$REPO_SLUG"
echo "Deploying on ${SSH_URL}"
echo "Fetching sources from ${GIT_URL}"

ssh "${SSH_URL}" "
git -C $BRANCH pull $BRANCH || git clone $GIT_URL $BRANCH
cd $BRANCH
git checkout $BRANCH
sh ./docker-ignite.sh
"
