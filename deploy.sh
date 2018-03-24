#!/usr/bin/env bash

# on lit GIT_DEPLOY_REPO ou si y‘a pas on tente le package.json repository field
# (il faut une string et pas un objet dans ce cas)
GIT_DEPLOY_REPO=${GIT_DEPLOY_REPO:-$(python -e 'process.stdout.write(require("./package.json").repository)')}

# on se déplace dans le dossier de build
cd dist && \
# on repart sur un repo git vierge, pas besoin de versionner quelque chose qui
# se build
$(npm bin)/rimraf .git
git init && \
# il faut quelques infos pour que git soit content
git config user.name "ounes" && \
git config user.email "ounes.chadli@gmail.com" && \
# on met tout dans git et on commit
git add . && \
git commit -m "Deploy From GitHub" && \
# puis on force push sur gh-pages
git push --force "${GIT_DEPLOY_REPO}" master:gh-pages