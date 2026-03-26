# cassandra-uploader

Запуск старой десктопной версии
cd /Users/ppa/Documents/CursorWorkDir/Cassandra-tool/v.0.1
mvn javafx:run

Запуск веб-приложения
cd /Users/ppa/Documents/CursorWorkDir/Cassandra-tool
mvn spring-boot:run

запуск с другим портом без правки файлов:
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081
