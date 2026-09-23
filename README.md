JSON-RPC Client
===============
![GitHub](https://img.shields.io/github/license/osslabz/json-rpc-client)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/osslabz/json-rpc-client/build-on-push.yml?branch=dev&label=build&logo=git)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/osslabz/json-rpc-client/release.yml?branch=dev&label=perform-release&logo=semanticrelease)
[![Reproducible Builds](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/jvm-repo-rebuild/reproducible-central/master/content/net/osslabz/json-rpc-client/badge.json)](https://github.com/jvm-repo-rebuild/reproducible-central/blob/master/content/net/osslabz/json-rpc-client/README.md)
[![Maven Central](https://img.shields.io/maven-central/v/net.osslabz/json-rpc-client?label=Maven%20Central)](https://search.maven.org/artifact/net.osslabz/json-rpc-client)

One author, four releases on Maven Central since March 2025, used by one other project of mine. Seventeen tests run
against an in-repo mock server and cover reconnects, timeouts and malformed responses, but the API is 0.x and can still
change.

Maven
------

```xml

<dependency>
    <groupId>net.osslabz</groupId>
    <artifactId>json-rpc-client</artifactId>
    <version>0.0.7</version>
</dependency>
```

Snapshots
---------

Every push to `dev` publishes the next version as a `-SNAPSHOT` to Central's snapshot repository. Maven doesn't
search that repository by default, so a build that wants a snapshot declares it:

```xml
<repositories>
    <repository>
        <id>central-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```
