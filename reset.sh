#!/bin/bash
set -e
echo "--- 15초 대기 (잔여 전달 소진) ---"
sleep 15

docker exec redis redis-cli FLUSHALL
docker exec mysql-master mysql -uroot -ppassword minichatdb -e "
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE messages;
TRUNCATE TABLE userchats;
TRUNCATE TABLE chats;
TRUNCATE TABLE fcm_token;
TRUNCATE TABLE users;
SET FOREIGN_KEY_CHECKS=1;"

echo "--- 검증 ---"
docker exec redis redis-cli DBSIZE
docker exec mysql-master mysql -uroot -ppassword minichatdb -e 'SELECT COUNT(*) FROM messages;'
echo "--- 시작 오프셋 (기록해두세요) ---"
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group chat-group