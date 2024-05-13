Unique IPs Counter

This is my implementation of test task for Ecwid: https://github.com/Ecwid/new-job/blob/master/IP-Addr-Counter.md

To launch the app do the following:

export JAVA_HOME=/Users/{UserName}/Library/Java/JavaVirtualMachines/corretto-17.0.11/Contents/Home
mvn clean package -DskipTests
java -jar target/ip-counter-1.0.jar /Users/{UserName}/IdeaProjects/ip-counter/src/test/resources/ips2.txt