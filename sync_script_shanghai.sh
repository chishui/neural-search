#!/bin/bash

SOURCE_DIR="./"
#REMOTE_USER="xiliyun"
#REMOTE_HOST="dev-dsk-xiliyun-1b-dd1be842.ap-northeast-1.amazon.com"
#REMOTE_DIR="/local/home/xiliyun/projects/test-lucene/"

REMOTE_USER="ubuntu"
REMOTE_HOST=$1
REMOTE_DIR="/home/$REMOTE_USER/neural-search"


rsync -avz -e "ssh -i /Users/xiliyun/Downloads/shanghaiopensearch-liyun.pem" --exclude-from='exclude.txt' --delete "$SOURCE_DIR" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR"
